import boto3

class S3Client:
    def __init__(self, config):
        self.bucket = config['S3']['bucket']
        self.client = boto3.client('s3', region_name=config['S3']['region'])

    def download_file(self, s3_key, local_file):
        self.client.download_file(self.bucket, s3_key, local_file)
