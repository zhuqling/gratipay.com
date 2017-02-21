# -*- coding: utf-8 -*-
from __future__ import absolute_import, division, print_function, unicode_literals

from gratipay.models.package import NPM, Package
from gratipay.testing import Harness
from gratipay.testing.emails import EmailHarness


class TestPackage(Harness):

    def test_can_be_instantiated_from_id(self):
        p = self.make_package()
        assert Package.from_id(p.id).id == p.id

    def test_can_be_instantiated_from_names(self):
        self.make_package()
        assert Package.from_names(NPM, 'foo').name == 'foo'


class TestClaiming(EmailHarness):

    def test_participant_can_initiate_package_claim(self):
        alice = self.make_participant('alice', claimed_time='now')
        p = self.make_package()
        alice.initiate_package_claim(p)
        assert self.get_last_email()
