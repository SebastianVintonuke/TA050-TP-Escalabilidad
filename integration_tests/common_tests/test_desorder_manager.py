import unittest
from common.state_manager.packet_id_tracker import PacketIDTracker

class PacketIDTrackerTEst(unittest.TestCase):


    def test_check_higher_than_expected_packet_id_adds_missing(self):
    	tracker = PacketIDTracker()

    	tracker.check_new_packet(5)

    	for i in range(1,5):
    		self.assertIn(i, tracker.missing_packets)

    	self.assertEqual(len(tracker.missing_packets),  4)
