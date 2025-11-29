import unittest
from common.state_storage.packet_id_tracker import PacketIDTracker


class PacketIDTrackerTest(unittest.TestCase):

    def test_initial_state(self):
        tracker = PacketIDTracker()
        self.assertEqual(tracker.expected_next_packet, 0)
        self.assertEqual(len(tracker.missing_packets), 0)

    def test_in_order_packets_do_not_add_missing(self):
        tracker = PacketIDTracker()

        tracker.check_new_packet(0)
        tracker.check_new_packet(1)
        tracker.check_new_packet(2)

        self.assertEqual(tracker.expected_next_packet, 3)
        self.assertEqual(len(tracker.missing_packets), 0)

    def test_check_higher_than_expected_packet_id_adds_missing(self):
        tracker = PacketIDTracker()

        tracker.check_new_packet(5)

        for i in range(0, 5):
            self.assertIn(i, tracker.missing_packets)

        self.assertEqual(len(tracker.missing_packets), 5)

    def test_packet_in_missing_is_consumed(self):
        tracker = PacketIDTracker()

        tracker.check_new_packet(5)
        # 0,1,2,3,4 now missing

        tracker.check_new_packet(2)  # Should remove from missing

        self.assertNotIn(2, tracker.missing_packets)

    def test_packet_lower_than_expected_but_not_missing_is_duplicate(self):
        tracker = PacketIDTracker()

        tracker.check_new_packet(5)  # missing = 0..4
        tracker.check_new_packet(2)  # now 2 removed
        # 0,1,3,4 remaining

        self.assertTrue(tracker.is_duplicate(2))   # 2 already consumed
        self.assertTrue(tracker.is_duplicate(1) == False)  # 1 is missing, so NOT duplicate yet

    def test_duplicate_when_no_missing(self):
        tracker = PacketIDTracker()

        tracker.check_new_packet(0)
        tracker.check_new_packet(1)

        self.assertTrue(tracker.is_duplicate(1))
        self.assertTrue(tracker.is_duplicate(0))

    def test_idempotent_removal_of_missing_packet(self):
        tracker = PacketIDTracker()

        tracker.check_new_packet(5)
        tracker.check_new_packet(1)
        tracker.check_new_packet(1)  # Should silently discard

        self.assertNotIn(1, tracker.missing_packets)

    def test_large_gap(self):
        tracker = PacketIDTracker()

        tracker.check_new_packet(1000)

        self.assertEqual(len(tracker.missing_packets), 1000)
        self.assertIn(999, tracker.missing_packets)
        self.assertIn(0, tracker.missing_packets)

    def test_filling_missing_packets_in_random_order(self):
        tracker = PacketIDTracker()

        tracker.check_new_packet(5)  # missing: 0,1,2,3,4

        tracker.check_new_packet(1)
        tracker.check_new_packet(4)
        tracker.check_new_packet(0)
        tracker.check_new_packet(3)
        tracker.check_new_packet(2)

        self.assertEqual(len(tracker.missing_packets), 0)
        self.assertEqual(tracker.expected_next_packet, 6)  # latest new was 5, so 6 expected

