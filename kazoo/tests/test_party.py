import uuid


from nose.tools import eq_

from kazoo.testing import KazooTestCase


class KazooPartyTests(KazooTestCase):
    def setUp(self):
        super(KazooPartyTests, self).setUp()
        self.path = "/" + uuid.uuid4().hex

    def test_party(self):
        parties = [self.client.Party(self.path, "p%s" % i)
                    for i in range(5)]

        one_party = parties[0]

        eq_(one_party.get_participants(), [])
        eq_(one_party.get_participant_count(), 0)

        participants = set()
        for party in parties:
            party.join()
            participants.add(party.data)

            eq_(set(party.get_participants()), participants)
            eq_(party.get_participant_count(), len(participants))

        for party in parties:
            party.leave()
            participants.remove(party.data)

            eq_(set(party.get_participants()), participants)
            eq_(party.get_participant_count(), len(participants))
