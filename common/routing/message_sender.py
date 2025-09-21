

class MessageSender:
	def __init__(self, chann, target):
		self.channel = chann
		self.target_key = target

	def send(self, msg):
		self.channel.basic_publish(exchange='', routing_key=self.target_key, body=msg)

