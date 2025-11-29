FINISH_ACTION = 2
NACK_ACTION = 1
ACK_ACTION = 0
ACCUMULATE_ACTION = 3


def nack_batch(ch, batch):
	for tag in batch:						
		# If msg failed, requeue is desired else throw exception(for now?)
		ch.basic_nack(delivery_tag=tag, requeue=True)
	batch.clear()	

def ack_batch(ch, batch):
	for tag in batch:						
		ch.basic_ack(delivery_tag=tag)
	batch.clear()	

def finish_batch(ch, batch):
	for tag in batch:						
		ch.basic_ack(delivery_tag=tag)
	batch.clear()	

# ACTION_HANDLERS = [
# 	ack_batch,
# 	nack_batch,
# 	finish_batch
# ]

class BatchingACKManager:
	def __init__(self):
		self.map_ack_groups = {}

	def add_to_ack_group(self, group, tag):
		res = self.map_ack_groups.setdefault(group, [])
		res.append(tag)
		return res

	def new_ack_info(self, new_tag, ch, ack_group, ack_act):
		batch = self.map_ack_groups.setdefault(ack_group, [])
		batch.append(new_tag)

		if ack_act == ACCUMULATE_ACTION:			
			return

		if ack_act == ACK_ACTION:
			ack_batch(ch, batch)
			return

		if ack_act == NACK_ACTION:
			nack_batch(ch, batch)
			return

		ack_batch(ch, batch)
		del self.map_ack_groups[ack_group]