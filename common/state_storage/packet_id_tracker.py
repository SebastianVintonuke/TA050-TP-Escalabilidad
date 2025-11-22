
class PacketIDTracker:
	def __init__(self):
		self.expected_next_packet = 0
		self.missing_packets = set()


	# If it is a duplicate it is discarded the packet
	def is_duplicate(self, packet_id):
		return packet_id < self.expected_next_packet and (not packet_id in self.missing_packets)

	## Checks packet id, updates missing packets and expected next packet 
	## Should be called only If not duplicate.. so if packet_id < exp_packet .. then should be on missing packets..
	def check_new_packet(self, packet_id):
		exp_packet = self.expected_next_packet
		
		# Optimistically this would always happen, in order reaching
		if packet_id == exp_packet:
			self.expected_next_packet = exp_packet+1
			return

		if packet_id > exp_packet:
			# Add missing packets
			for i in range(exp_packet, packet_id):
				self.missing_packets.add(i)
			
			self.expected_next_packet = packet_id+1 # Expect next one.
			return

		# Should be guaranteed to be on missing but still just discard If found, do not throw error. Idempotent
		self.missing_packets.discard(packet_id)