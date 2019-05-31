# utils
import boto3
import json
import pandas as pd
# secrets
from credentials import credentials as credentials

class Store:
    def __init__(self, performance_monitor, bucket_name='forecasting-pipeline-files'):
        self.performance_monitor = performance_monitor
        self.bucket_name = bucket_name
        self.client = boto3.client(
            's3',
            aws_access_key_id=credentials['aws_access_key'],
            aws_secret_access_key=credentials['aws_secret_key']
        )


    def get_json(self, key):
        local_filename = key.split('/')[-1]
        self.client.download_file(self.bucket_name, key, local_filename)
        self.performance_monitor.log_infile(local_filename)
        result = self.client.get_object(Bucket=self.bucket_name, Key=key)
        return json.loads(result["Body"].read().decode())


    def get_pickle(self, key):
        local_filename = key.split('/')[-1]
        self.client.download_file(self.bucket_name, key, local_filename)
        self.performance_monitor.log_infile(local_filename)
        return pd.read_pickle(local_filename)


    def get_csv(self, key):
        local_filename = key.split('/')[-1]
        self.client.download_file(self.bucket_name, key, local_filename)
        self.performance_monitor.log_infile(local_filename)
        try:
            with open(local_filename, 'r') as f:
                return pd.read_csv(f, delimiter=',')
        except FileNotFoundError:
            return {}


    def save_file(self, key):
        local_filename = key.split('/')[-1]
        self.performance_monitor.log_outfile(local_filename)
        response = self.client.upload_file(local_filename, self.bucket_name, key)


def main():
    s = Store('forecasting-pipeline-files')


if __name__ == "__main__":

    main()