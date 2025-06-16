from configparser import ConfigParser
from messaging.mq_client import MQClient

config = ConfigParser()
config.read('config.ini')

mq = MQClient(config)

# Send a message
mq.send_message('Test message from Python')

# Receive one message
msg = mq.receive_message()
print(f"Received: {msg}")

# Send/Receive from file
mq.send_messages_from_file('input.txt')
mq.receive_messages_to_file('output.txt')
