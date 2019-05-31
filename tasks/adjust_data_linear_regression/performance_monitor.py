# python libs
import time
import threading
import json
import datetime
import os
# import requests
# external utils
import psutil
import boto3
import pika
# secrets
from credentials import credentials as credentials

def get_fargate_metrics_object(cpu, ram, elapsed_time, previous_result):
    # Fargate service cost per second
    FARGATE_CPU_COST = 0.04048 / 60 / 60 
    FARGATE_RAM_COST = 0.004445 / 60 / 60
    if previous_result and 'cost_usd' in previous_result:
        cpu_cost = previous_result['cost_cpu'] + cpu * FARGATE_CPU_COST
        ram_cost = previous_result['cost_ram'] + (ram['used']/1024/1024/1024) * FARGATE_RAM_COST * elapsed_time
    else:
        cpu_cost = cpu * FARGATE_CPU_COST
        ram_cost = (ram['used']/1024/1024/1024) * FARGATE_RAM_COST * elapsed_time
    
    return { 'cost_cpu': cpu_cost, 'cost_ram': ram_cost, 'cost_usd': ram_cost + cpu_cost }


class PerformanceMonitor:
    def __init__(self, task_name, pipeline_id, queue_name='performance_monitor', interval=1, define_metrics_object=get_fargate_metrics_object, credentials=credentials):
        self.task_name = task_name
        self.pipeline_id = pipeline_id
        self.stopped = False
        self.time = 0
        # self.cpu_cost = 0
        # self.ram_cost = 0
        self.results = []
        self.interval = interval
        self.queue_name = queue_name
        self.get_custom_metrics_object = define_metrics_object
        self.credentials = credentials
        self.data_sizes = {
            'in': {},
            'out': {},
            'task_name': task_name,
            'pipeline_id': pipeline_id,
            'date': datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        }
        self.s3_client = boto3.client(
            's3',
            aws_access_key_id=self.credentials['aws_access_key'],
            aws_secret_access_key=self.credentials['aws_secret_key']
        )


    def stop(self):
        print('DATA SIZE:')
        print(self.data_sizes)
        self.stopped = True
        print('Elapsed time', self.time)
        metrics_object = { **self.get_metrics_object(), **{ 'final': True } }
        self.results.append(metrics_object)
        self.save_results()
        self.save_data_logs()
        self.push_json_to_queue(self.queue_name, json.dumps(metrics_object))
        self.push_json_to_queue(self.queue_name, json.dumps({**self.data_sizes, **{ 'metric_type': 'data_logs' }}))


    def start(self):
        thread = threading.Thread(target=self.listen_to_resources)
        thread.start()


    def get_metrics_object(self):
        cpu = psutil.cpu_percent()
        ram = dict(psutil.virtual_memory()._asdict())
        t = datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S")
        previous_result = self.results[-1] if len(self.results) > 0 else None

        default = { 'cpu': cpu, 'ram': ram, 'date': t, 'time_elapsed': self.time, 'task_name': self.task_name, 'pipeline_id': self.pipeline_id, 'metric_type': 'metrics' }

        return {**default, **self.get_custom_metrics_object(cpu, ram, self.time, previous_result)}


    def listen_to_resources(self):
        while not self.stopped:
            self.time += self.interval
            metrics_object = self.get_metrics_object()
            self.results.append(metrics_object)
            self.push_json_to_queue(self.queue_name, json.dumps(metrics_object))
            time.sleep(self.interval)


    def push_json_to_queue(self, queue, json):
        rabbitmq_credentials = pika.PlainCredentials(self.credentials['rabbitmq_user'], self.credentials['rabbitmq_password'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.credentials['rabbitmq_host'], credentials=rabbitmq_credentials))
        channel = connection.channel()

        channel.queue_declare(queue=queue)

        channel.basic_publish(exchange='', routing_key=queue, body=json)
        print('Sent: ' + json)
        connection.close()


    def save_results(self):
        self.save_object_to_s3(self.results, 'performance-')


    def save_data_logs(self):
        self.save_object_to_s3(self.data_sizes, 'data-logs-')


    def save_object_to_s3(self, object, prefix):
        body = (bytes(json.dumps(object, indent=2).encode('UTF-8')))
        key = prefix + self.task_name.replace('_', '-') + str(datetime.datetime.now()).replace(' ', '-') + '.json'
        response = self.s3_client.put_object(Body=body, Bucket='forecasting-pipeline-metrics', Key=key)
        print(response)


    def log_infile(self, path):
        size = os.path.getsize(path)
        self.data_sizes['in'][path] = size


    def log_outfile(self, path):
        size = os.path.getsize(path)
        self.data_sizes['out'][path] = size


    def log_analytics_metric(self, payload):
        metrics_object = {
            'payload': payload,
            'task_name': self.task_name,
            'pipeline_id': self.pipeline_id,
            'date': datetime.datetime.now().strftime("%Y/%m/%d %H:%M:%S"),
            'metric_type': 'analytics'
        }
        self.push_json_to_queue(self.queue_name, json.dumps(metrics_object))

