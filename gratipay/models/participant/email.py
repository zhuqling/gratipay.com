# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

import pickle
import uuid
from datetime import timedelta
from time import sleep

from aspen.utils import utcnow
from markupsafe import escape as htmlescape
from psycopg2 import IntegrityError

import gratipay
from gratipay.exceptions import EmailAlreadyTaken, CannotRemovePrimaryEmail, EmailNotVerified
from gratipay.exceptions import TooManyEmailAddresses, ResendingTooFast
from gratipay.models import add_event
from gratipay.security.crypto import constant_time_compare
from gratipay.utils import encode_for_querystring, i18n


EMAIL_HASH_TIMEOUT = timedelta(hours=24)

( VERIFICATION_MISSING
, VERIFICATION_FAILED
, VERIFICATION_EXPIRED
, VERIFICATION_REDUNDANT
, VERIFICATION_STYMIED
, VERIFICATION_SUCCEEDED
 ) = range(6)


class Email(object):
    """Participants may associate email addresses with their account.

    Email addresses are stored in an ``emails`` table in the database, which
    holds the addresses themselves as well as info related to address
    verification. While a participant may have multiple email addresses on
    file, verified or not, only one will be the *primary* email address: the
    one also recorded in ``participants.email_address``. It's a bug for the
    primary address not to be verified, or for an address to be in
    ``participants.email_address`` but not also in ``emails``.

    Having a verified email is a prerequisite for certain other features on
    Gratipay, such as linking a PayPal account, or filing a national identity.

    """

    def add_email(self, email, resend_threshold='3 minutes'):
        """Add an email address for a participant.

        This is called when adding a new email address, and when resending the
        verification email for an unverified email address.

        :returns: the number of emails sent.

        """

        # Check that this address isn't already verified
        owner = self.db.one("""
            SELECT p.username
              FROM emails e INNER JOIN participants p
                ON e.participant_id = p.id
             WHERE e.address = %(email)s
               AND e.verified IS true
        """, locals())
        if owner:
            if owner == self.username:
                return 0
            else:
                raise EmailAlreadyTaken(email)

        if len(self.get_emails()) > 9:
            raise TooManyEmailAddresses(email)

        nonce = str(uuid.uuid4())
        verification_start = utcnow()

        nrecent = self.db.one( "SELECT count(*) FROM emails WHERE address=%s AND "
                               "%s - verification_start < %s"
                             , (email, verification_start, resend_threshold)
                              )
        if nrecent:
            raise ResendingTooFast()

        try:
            with self.db.get_cursor() as c:
                add_event(c, 'participant', dict(id=self.id, action='add', values=dict(email=email)))
                c.run("""
                    INSERT INTO emails
                                (address, nonce, verification_start, participant_id)
                         VALUES (%s, %s, %s, %s)
                """, (email, nonce, verification_start, self.id))
        except IntegrityError:
            nonce = self.db.one("""
                UPDATE emails
                   SET verification_start=%s
                 WHERE participant_id=%s
                   AND address=%s
                   AND verified IS NULL
             RETURNING nonce
            """, (verification_start, self.id, email))
            if not nonce:
                return self.add_email(email)

        base_url = gratipay.base_url
        username = self.username_lower
        encoded_email = encode_for_querystring(email)
        link = "{base_url}/~{username}/emails/verify.html?email2={encoded_email}&nonce={nonce}"
        r = self.send_email('verification',
                            email=email,
                            link=link.format(**locals()),
                            include_unsubscribe=False)
        assert r == 1 # Make sure the verification email was sent
        if self.email_address:
            self.send_email('verification_notice',
                            new_email=email,
                            include_unsubscribe=False)
            return 2
        return 1


    def update_email(self, email):
        """Set the email address for the participant.
        """
        if not getattr(self.get_email(email), 'verified', False):
            raise EmailNotVerified(email)
        username = self.username
        with self.db.get_cursor() as c:
            add_event(c, 'participant', dict(id=self.id, action='set', values=dict(primary_email=email)))
            c.run("""
                UPDATE participants
                   SET email_address=%(email)s
                 WHERE username=%(username)s
            """, locals())
        self.set_attributes(email_address=email)


    def verify_email(self, email, nonce):
        if '' in (email, nonce):
            return VERIFICATION_MISSING
        r = self.get_email(email)
        if r is None:
            return VERIFICATION_FAILED
        if r.verified:
            assert r.nonce is None  # and therefore, order of conditions matters
            return VERIFICATION_REDUNDANT
        if not constant_time_compare(r.nonce, nonce):
            return VERIFICATION_FAILED
        if (utcnow() - r.verification_start) > EMAIL_HASH_TIMEOUT:
            return VERIFICATION_EXPIRED
        try:
            self.db.run("""
                UPDATE emails
                   SET verified=true, verification_end=now(), nonce=NULL
                 WHERE participant_id=%s
                   AND address=%s
                   AND verified IS NULL
            """, (self.id, email))
        except IntegrityError:
            return VERIFICATION_STYMIED

        if not self.email_address:
            self.update_email(email)
        return VERIFICATION_SUCCEEDED


    def get_email(self, email):
        """Return a record for a single email address on file for this participant.
        """
        return self.db.one("""
            SELECT *
              FROM emails
             WHERE participant_id=%s
               AND address=%s
        """, (self.id, email))


    def get_emails(self):
        """Return a list of all email addresses on file for this participant.
        """
        return self.db.all("""
            SELECT *
              FROM emails
             WHERE participant_id=%s
          ORDER BY id
        """, (self.id,))


    def get_verified_email_addresses(self):
        """Return a list of verified email addresses on file for this participant.
        """
        return [email.address for email in self.get_emails() if email.verified]


    def remove_email(self, address):
        """Remove the given email address from the participant's account.
        Raises ``CannotRemovePrimaryEmail`` if the address is primary. It's a
        noop if the email address is not on file.
        """
        if address == self.email_address:
            raise CannotRemovePrimaryEmail()
        with self.db.get_cursor() as c:
            add_event(c, 'participant', dict(id=self.id, action='remove', values=dict(email=address)))
            c.run("DELETE FROM emails WHERE participant_id=%s AND address=%s",
                  (self.id, address))


    def queue_email(self, spt_name, **context):
        """Given the name of a template under ``emails/`` and context in
        kwargs, queue a message to be sent.
        """
        self.db.run("""
            INSERT INTO email_queue
                        (participant, spt_name, context)
                 VALUES (%s, %s, %s)
        """, (self.id, spt_name, pickle.dumps(context)))


    @classmethod
    def dequeue_emails(cls):
        """Load messages queued for sending, and send them.
        """
        fetch_messages = lambda: cls.db.all("""
            SELECT *
              FROM email_queue
          ORDER BY id ASC
             LIMIT 60
        """)
        nsent = 0
        while True:
            messages = fetch_messages()
            if not messages:
                break
            for msg in messages:
                p = cls.from_id(msg.participant)
                r = p.send_email(msg.spt_name, **pickle.loads(msg.context))
                cls.db.run("DELETE FROM email_queue WHERE id = %s", (msg.id,))
                if r == 1:
                    sleep(1)
                nsent += r
        return nsent


    def send_email(self, spt_name, **context):
        """Given the name of an email template and context in kwargs, send an
        email using the configured mailer.
        """
        context['participant'] = self
        context['username'] = self.username
        context['button_style'] = (
            "color: #fff; text-decoration:none; display:inline-block; "
            "padding: 0 15px; background: #396; white-space: nowrap; "
            "font: normal 14px/40px Arial, sans-serif; border-radius: 3px"
        )
        context.setdefault('include_unsubscribe', True)
        email = context.setdefault('email', self.email_address)
        if not email:
            return 0 # Not Sent
        langs = i18n.parse_accept_lang(self.email_lang or 'en')
        locale = i18n.match_lang(langs)
        i18n.add_helpers_to_context(self._tell_sentry, context, locale)
        context['escape'] = lambda s: s
        context_html = dict(context)
        i18n.add_helpers_to_context(self._tell_sentry, context_html, locale)
        context_html['escape'] = htmlescape
        spt = self._emails[spt_name]
        base_spt = self._emails['base']
        def render(t, context):
            b = base_spt[t].render(context).strip()
            return b.replace('$body', spt[t].render(context).strip())

        message = {}
        message['Source'] = 'Gratipay Support <support@gratipay.com>'
        message['Destination'] = {}
        message['Destination']['ToAddresses'] = ["%s <%s>" % (self.username, email)] # "Name <email@domain.com>"
        message['Message'] = {}
        message['Message']['Subject'] = {}
        message['Message']['Subject']['Data'] = spt['subject'].render(context).strip()
        message['Message']['Body'] = {
            'Text': {
                'Data': render('text/plain', context)
            },
            'Html': {
                'Data': render('text/html', context_html)
            }
        }

        self._mailer.send_email(**message)
        return 1 # Sent


    def set_email_lang(self, accept_lang):
        """Given a language identifier, set it for the participant as their
        preferred language in which to receive email.
        """
        if not accept_lang:
            return
        self.db.run("UPDATE participants SET email_lang=%s WHERE id=%s",
                    (accept_lang, self.id))
        self.set_attributes(email_lang=accept_lang)
