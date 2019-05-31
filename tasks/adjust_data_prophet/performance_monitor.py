# python libs
import time
import threading
import json
import datetime
# external utils
import psutil
import boto3
# secrets
from credentials import credentials as credentials

# Fargate service cost per second
FARGATE_CPU_COST = 0.04048 / 60 / 60 
FARGATE_RAM_COST = 0.004445 / 60 / 60
INTERVAL = 1

class PerformanceMonitor:
    def __init__(self, name):
        self.name = name
        self.stopped = False
        self.time = 0
        self.cpu_cost = 0
        self.ram_cost = 0
        self.results = []
        self.cloudwatch_client = boto3.client(
            'cloudwatch',
            aws_access_key_id=credentials['access_key'],
            aws_secret_access_key=credentials['secret_key']
        )
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=credentials['access_key'],
            aws_secret_access_key=credentials['secret_key']
        )


    def stop(self):
        self.stopped = True
        print('Elapsed time', self.time)
        self.push_metrics_to_cloudwatch()
        self.save_results()


    def start(self):
        thread = threading.Thread(target=self.listen_to_resources)
        thread.start()


    def listen_to_resources(self):
        while not self.stopped:
            cpu = psutil.cpu_percent()
            self.cpu_cost += cpu * FARGATE_CPU_COST
            ram = dict(psutil.virtual_memory()._asdict())
            self.ram_cost = (ram['used']/1024/1024/1024) * FARGATE_RAM_COST * INTERVAL
            t = str(datetime.datetime.now())
            self.time += INTERVAL
            self.results.append({ 'cpu': cpu, 'ram': ram, 'time': t, 'time_elapsed': self.time, 'cost_usd': self.ram_cost + self.cpu_cost })            
            self.push_metrics_to_cloudwatch()
            time.sleep(INTERVAL)


    def push_metrics_to_cloudwatch(self):
        response = self.cloudwatch_client.put_metric_data(
            MetricData = [
                {
                    'MetricName': 'EstimatedCost',
                    'Dimensions': [
                        {
                            'Name': 'TASK_NAME',
                            'Value': self.name
                        }
                    ],
                    'Unit': 'None',
                    'Value': (self.cpu_cost + self.ram_cost) * 100,
                    'StorageResolution': 1
                },
            ],
            Namespace = 'AirflowTestMetrics'
        )
        print(response)


    def save_results(self):
        body = (bytes(json.dumps(self.results, indent=2).encode('UTF-8')))
        key = 'performance-' + self.name + str(datetime.datetime.now()).replace(' ', '-') + '.json'
        response = self.s3_client.put_object(Body=body, Bucket='forecasting-pipeline-metrics', Key=key)
        print(response)


# def main():
#     pm = PerformanceMonitor('test')
#     pm.start()
#     j = 0
#     for i in range(1000000):
#         print(i)
#     pm.stop()

# if __name__ == "__main__":

#     main()

