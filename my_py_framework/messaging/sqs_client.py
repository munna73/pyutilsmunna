import boto3
import json

class SQSClient:
    def __init__(self, config):
        self.queue_url = config['SQS']['queue_url']
        self.client = boto3.client('sqs', region_name=config['SQS']['region'])

    def send_messages_from_file(self, filepath):
        with open(filepath) as f:
            for line in f:
                self.client.send_message(QueueUrl=self.queue_url, MessageBody=line.strip())

    def receive_and_save_messages(self, out_file):
        with open(out_file, 'w') as f:
            for _ in range(10):  # fetch 10 batches
                messages = self.client.receive_message(
                    QueueUrl=self.queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2
                ).get('Messages', [])
                for msg in messages:
                    f.write(msg['Body'] + "\n")
                    self.client.delete_message(QueueUrl=self.queue_url, ReceiptHandle=msg['ReceiptHandle'])
