import pymqi
import os

class MQClient:
    def __init__(self, config):
        mq_config = config['MQ']
        self.queue_manager = mq_config.get('queue_manager', 'QM1')
        self.channel = mq_config['channel']
        self.host = mq_config['host']
        self.port = mq_config.get('port', '1414')
        self.queue_name = mq_config['queue_name']

        self.conn_info = f"{self.host}({self.port})"
        self.queue = None
        self.qmgr = None

    def connect(self):
        """Establish connection to the MQ queue manager."""
        self.qmgr = pymqi.connect(
            self.queue_manager,
            self.channel,
            self.conn_info
        )
        self.queue = pymqi.Queue(self.qmgr, self.queue_name)

    def disconnect(self):
        """Disconnect from the MQ server."""
        if self.queue:
            self.queue.close()
        if self.qmgr:
            self.qmgr.disconnect()

    def send_message(self, message: str):
        """Send a message to the MQ queue."""
        if not self.queue:
            self.connect()
        self.queue.put(message)

    def receive_message(self, wait_interval=5):
        """
        Receive one message from the MQ queue.
        Returns the message or None if timeout occurs.
        """
        if not self.queue:
            self.connect()

        gmo = pymqi.GMO()
        gmo.Options = pymqi.CMQC.MQGMO_WAIT | pymqi.CMQC.MQGMO_FAIL_IF_QUIESCING
        gmo.WaitInterval = wait_interval * 1000  # milliseconds

        try:
            message = self.queue.get(None, gmo)
            return message
        except pymqi.MQMIError as e:
            if e.reason == pymqi.CMQC.MQRC_NO_MSG_AVAILABLE:
                return None
            else:
                raise

    def send_messages_from_file(self, filepath):
        with open(filepath, 'r') as file:
            for line in file:
                message = line.strip()
                if message:
                    self.send_message(message)

    def receive_messages_to_file(self, out_file, max_messages=10):
        with open(out_file, 'w') as file:
            for _ in range(max_messages):
                message = self.receive_message()
                if message:
                    file.write(message + '\n')
                else:
                    break
