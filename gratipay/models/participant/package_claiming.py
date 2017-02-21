# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals


class PackageClaiming(object):

    """Gratipay participants may claim packages on the Node package manager
    (npm), bringing them into Gratipay as projects similar to any other. The
    claiming process is handled via email: ``initiate_package_claim`` sends an
    email to an address registered with npm, and a link back from the email
    lands in ``claim_package`` to finalize the claim.

    Packages can also be unclaimed, and reclaimed.

    """

    def initiate_package_claim(self, package, email):
        """Initiate a claim on the given package.

        :param Package package: a ``Package`` instance

        :returns: ``None``

        """
        assert email in package.emails  # sanity check

        r = self.send_email('claim_package',
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
