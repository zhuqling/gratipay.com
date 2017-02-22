#!/usr/bin/env python
"""This is a script for managing MassPay each week.

See documentation here:

    http://inside.gratipay.com/howto/run-masspay

"""
from __future__ import absolute_import, division, print_function, unicode_literals

import csv
import datetime
import getpass
import os
import sys
from decimal import Decimal as D, ROUND_HALF_UP

import requests
from gratipay import wireup
from gratipay.billing.exchanges import get_ready_payout_routes_by_network
from httplib import IncompleteRead


os.chdir('../logs/masspay')
ts = datetime.datetime.now().strftime('%Y-%m-%d')
INPUT_CSV = '{}.input.csv'.format(ts)
PAYPAL_CSV = '{}.output.paypal.csv'.format(ts)
GRATIPAY_CSV = '{}.output.gratipay.csv'.format(ts)
REPORT_CSV = '{}.report.paypal.csv'.format(ts)


def round_(d):
    return d.quantize(D('0.01'), rounding=ROUND_HALF_UP)

def print_rule(w=80):
    print("-" * w)


class Payee(object):
    username = None
    route_id = None
    email = None
    gross = None
    gross_perc = None
    fee = None
    net = None
    additional_note = ""

    def __init__(self, rec):
        self.username, self.route_id, self.email, fee_cap, amount = rec
        self.gross = D(amount)
        self.fee = D(0)
        self.fee_cap = D(fee_cap)
        self.net = self.gross

    def assess_fee(self):

        # In order to avoid slowly leaking escrow, we need to be careful about
        # how we compute the fee. It's complicated, but it goes something like
        # this:
        #
        #   1. We want to pass PayPal's fees through to each payee.
        #
        #   2. With MassPay there is no option to have the receiver pay the fee,
        #       as there is with Instant Transfer.
        #
        #   3. We have to subtract the fee before uploading the spreadsheet
        #       to PayPal.
        #
        #   4. If we upload 15.24, PayPal upcharges to 15.54.
        #
        #   6. If we upload 15.25, PayPal upcharges to 15.56.
        #
        #   7. They only accept whole cents. We can't upload 15.245.
        #
        #   8. What if we want to hit 15.55?
        #
        #   9. We can't.
        #
        #  10. Our solution is to leave a penny behind in Gratipay for
        #       affected payees.
        #
        #  11. BUT ... if we upload 1.25, PayPal upcharges to 1.28. Think about
        #       it.
        #
        # See also: https://github.com/gratipay/gratipay.com/issues/1673
        #           https://github.com/gratipay/gratipay.com/issues/2029
        #           https://github.com/gratipay/gratipay.com/issues/2198
        #           https://github.com/gratipay/gratipay.com/pull/2209
        #           https://github.com/gratipay/gratipay.com/issues/2296

        target = net = self.gross
        while 1:
            net -= D('0.01')
            fee = round_(net * D('0.02'))
            fee = min(fee, self.fee_cap)
            gross = net + fee
            if gross <= target:
                break
        self.gross = gross
        self.net = net
        self.fee = fee

        remainder = target - gross
        if remainder > 0:
            n = "{:.2} remaining due to PayPal rounding limitation.".format(remainder)
            self.additional_note = n

        return fee


def compute_input_csv():
    db = wireup.db(wireup.env())
    routes = get_ready_payout_routes_by_network(db, 'paypal')
    writer = csv.writer(open(INPUT_CSV, 'w+'))
    print_rule(88)
    headers = "username", "email", "fee cap", "amount"
    print("{:<24}{:<32} {:^7} {:^7}".format(*headers))
    print_rule(88)
    total_gross = 0
    for route in routes:
        amount = route.participant.balance
        if amount < 0.50:
            # Minimum payout of 50 cents. I think that otherwise PayPal upcharges to a penny.
            # See https://github.com/gratipay/gratipay.com/issues/1958.
            continue
        total_gross += amount
        print("{:<24}{:<32} {:>7} {:>7}".format( route.participant.username
                                                           , route.address
                                                           , route.fee_cap
                                                           , amount
                                                            ))
        row = (route.participant.username, route.id, route.address, route.fee_cap, amount)
        writer.writerow(row)
    print(" "*64, "-"*7)
    print("{:>72}".format(total_gross))


def compute_output_csvs():
    payees = [Payee(rec) for rec in csv.reader(open(INPUT_CSV))]
    payees.sort(key=lambda o: o.gross, reverse=True)

    total_fees = sum([payee.assess_fee() for payee in payees])  # side-effective!
    total_net = sum([p.net for p in payees])
    total_gross = sum([p.gross for p in payees])
    assert total_fees + total_net == total_gross

    paypal_csv = csv.writer(open(PAYPAL_CSV, 'w+'))
    gratipay_csv = csv.writer(open(GRATIPAY_CSV, 'w+'))
    print_rule()
    print("{:<24}{:<32} {:^7} {:^7} {:^7}".format("username", "email", "gross", "fee", "net"))
    print_rule()
    for payee in payees:
        paypal_csv.writerow((payee.email, payee.net, "usd"))
        gratipay_csv.writerow(( payee.username
                            , payee.route_id
                            , payee.email
                            , payee.gross
                            , payee.fee
                            , payee.net
                            , payee.additional_note
                             ))
        print("{username:<24}{email:<32} {gross:>7} {fee:>7} {net:>7}".format(**payee.__dict__))

    print(" "*56, "-"*23)
    print("{:>64} {:>7} {:>7}".format(total_gross, total_fees, total_net))


def load_statuses():
    _status_map = { 'Completed': 'succeeded'
                  , 'Unclaimed': 'pending'
                  , 'Denied': 'failed'
                   } # PayPal -> Gratipay
    statuses = {}
    fp = open(REPORT_CSV)
    for line in fp:
        if line.startswith('Transaction ID,Recipient'):
            break
    for rec in csv.reader(fp):
        statuses[rec[1]] = _status_map[rec[5]]
    return statuses


def post_back_to_gratipay(force=False):

    try:
        gratipay_api_key = os.environ['GRATIPAY_API_KEY']
    except KeyError:
        gratipay_api_key = getpass.getpass("Your admin user Gratipay API key: ")

    try:
        gratipay_base_url = os.environ['GRATIPAY_BASE_URL']
    except KeyError:
        gratipay_base_url = 'https://gratipay.com'

    nmasspays = int(requests.get(gratipay_base_url + '/dashboard/nmasspays').text())
    if nmasspays < 10 and not force:
        print("It looks like we didn't run MassPay last week! If you are absolutely sure that we "
              "did, then rerun with -f.")
        return

    statuses = load_statuses()

    nposts = 0
    for username, route_id, email, gross, fee, net, additional_note in csv.reader(open(GRATIPAY_CSV)):
        url = '{}/~{}/history/record-an-exchange'.format(gratipay_base_url, username)
        note = 'PayPal MassPay to {}.'.format(email)
        if additional_note:
            note += " " + additional_note
        print(note)
        status = statuses[email]

        data = {'amount': '-' + net, 'fee': fee, 'note': note, 'status': status, 'route_id': route_id}
        try:
            response = requests.post(url, auth=(gratipay_api_key, ''), data=data)
        except IncompleteRead:
            print('IncompleteRead, proceeding (but double-check!)')
        else:
            if response.status_code == 200:
                nposts += 1
            else:
                if response.status_code == 404:
                    print('Got 404, is your API key good? {}'.format(gratipay_api_key))
                else:
                    print('... resulted in a {} response:'.format(response.status_code))
                    print(response.text)
                raise SystemExit
        print("POSTed MassPay back to Gratipay for {} users.".format(nposts))


def run_report():
    """Print a report to help Determine how much escrow we should store in PayPal.
    """
    totals = []
    max_masspay = max_weekly_growth = D(0)
    for filename in os.listdir('.'):
        if not filename.endswith('.input.csv'):
            continue

        datestamp = filename.split('.')[0]

        totals.append(D(0))
        for rec in csv.reader(open(filename)):
            amount = rec[-1]
            totals[-1] += D(amount)

        max_masspay = max(max_masspay, totals[-1])
        if len(totals) == 1:
            print("{} {:8}".format(datestamp, totals[-1]))
        else:
            weekly_growth = totals[-1] / totals[-2]
            max_weekly_growth = max(max_weekly_growth, weekly_growth)
            print("{} {:8} {:4.1f}".format(datestamp, totals[-1], weekly_growth))

    print()
    print("Max Withdrawal:    ${:9,.2f}".format(max_masspay))
    print("Max Weekly Growth:  {:8.1f}".format(max_weekly_growth))
    print("5x Current:        ${:9,.2f}".format(5 * totals[-1]))


def main():
    if not sys.argv[1:]:
        print("Looking for files for {} ...".format(ts))
        for filename in (INPUT_CSV, PAYPAL_CSV, GRATIPAY_CSV):
            print("  [{}] {}".format('x' if os.path.exists(filename) else ' ', filename))
        print("Rerun with one of these options:")
        print("  -i - hits db to generate input CSV (needs envvars via heroku + honcho)")
        print("  -o - computes output CSVs (doesn't need anything but input CSV)")
        print("  -p - posts back to Gratipay (prompts for API key)")
    elif '-i' in sys.argv:
        compute_input_csv()
    elif '-o' in sys.argv:
        compute_output_csvs()
    elif '-p' in sys.argv:
        post_back_to_gratipay('-f' in sys.argv)


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        print()
