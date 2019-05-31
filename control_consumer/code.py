# qoa_toolkit.py
#  - performance_monitor - user can pass callback and credentials
#  - s3_store - user can pass credentials
#  - consumer - user can pass callback and credentials
#  - utils - module for utlity functions such as get args params

# python imports
import json
import requests
import time
# queue lib
import pika
# secrets
from credentials import credentials as credentials

def default_get_control_action(body_dict):
    index = body_dict.pop('metric_type', None)
    try:
        if index == 'metrics':
            if body_dict['cost_usd'] > 1 or body_dict['time_elapsed'] > 5:
                return 'SOFT_STOP'
            elif body_dict['time_elapsed'] > 8:
                return 'HARD_STOP'
        
        elif index == 'data_logs':
            if body_dict['task_name'] == 'clean_data':
                if body_dict['in']['train.csv'] / 2 > body_dict['out']['train.csv']:
                    return 'HARD_STOP'

        elif index == 'analytics':
            if body_dict['payload']['r2_squared'] < 0.5:
                return 'SOFT_STOP'

        else:
            print('No valid index found!')
            return -1
    except KeyError:
        pass


HARD_STOP_KEY = 'HARD_STOP'
SOFT_STOP_KEY = 'SOFT_STOP'

class ControlConsumer:
    def __init__(self, queue_name, credentials=credentials, control_action=default_get_control_action):
        self.queue_name = queue_name
        self.credentials = credentials
        self.control_action = control_action


    def start(self):
        print('Initialized consumer listener', flush=True)
        # Wait for the system to initialize
        time.sleep(180)

        print('Started consumer listener')

        rabbitmq_credentials = pika.PlainCredentials(self.credentials['rabbitmq_user'], self.credentials['rabbitmq_password'])
        connection = pika.BlockingConnection(pika.ConnectionParameters(self.credentials['rabbitmq_host'], self.credentials['rabbitmq_port'], '/', rabbitmq_credentials)) 
        channel = connection.channel()

        channel.queue_declare(queue=self.queue_name)

        channel.basic_consume(queue=self.queue_name, on_message_callback=self.callback, auto_ack=True)

        print('[*] Waiting for messages.')
        channel.start_consuming()


    def callback(self, ch, method, properties, body):
        print(" [x] Received %r" % body)
        body_dict = json.loads(body)

        if not body_dict['pipeline_id']:
            print('Undefined pipeline_id!')
            return -1

        action = self.control_action(body_dict)

        if action == HARD_STOP_KEY:
            self.stop_pipeline(body_dict['pipeline_id'])
        elif action == SOFT_STOP_KEY:
            self.pause_pipeline(body_dict['pipeline_id'])


    def pause_pipeline(self, pausable_pipeline_id):
        url = self.credentials['airflow_host'] + ':' + self.credentials['airflow_port'] + '/api/experimental/dags/' + pausable_pipeline_id + '/paused/true'
        response = requests.get(url)
        print(response)


    def stop_pipeline(self, stoppable_pipeline_id):
        url = self.credentials['flower_host'] + ':' + self.credentials['flower_port'] + '/api/tasks'
        response = requests.get(url)
        response_dict = json.loads(response.text)

        uuid_mapping = {}
        for key in response_dict:        
            uuid = response_dict[key]['uuid']
            args_array = response_dict[key]['args'].split(',')
            pipeline_id = args_array[2].replace("'", "").strip()
            task_name = args_array[3].replace("'", "").strip()
            print(uuid)

            if stoppable_pipeline_id != pipeline_id:
                continue

            if not pipeline_id in uuid_mapping:
                uuid_mapping[pipeline_id] = {}

            if not task_name in uuid_mapping[pipeline_id]:
                uuid_mapping[pipeline_id][task_name] = uuid

            r_url = self.credentials['flower_host'] + ':' + self.credentials['flower_port'] + '/api/task/revoke/' + uuid_mapping[pipeline_id][task_name] + '?terminate=true'
            r = requests.post(r_url)
            print(r)


# def callback(ch, method, properties, body):
#     print(" [x] Received %r" % body)
#     body_dict = json.loads(body)

#     if not body_dict['pipeline_id']:
#         print('Undefined pipeline_id!')
#         return -1

#     action = default_get_control_action(body_dict)

#     if action == HARD_STOP_KEY:
#         stop_pipeline(body_dict['pipeline_id'])
#     elif action == SOFT_STOP_KEY:
#         pause_pipeline(body_dict['pipeline_id'])


# def metrics_callback(metrics):
#     if metrics['cost_usd'] > 1 or metrics['time_elapsed'] > 5:
#         response = pause_pipeline(metrics['pipeline_id'])


# def data_logs_callback(data_logs):
#     if data_logs['task_name'] == 'clean_data':
#         if data_logs['in']['train.csv'] / 2 > data_logs['out']['train.csv']:
#             pause_pipeline(data_logs['pipeline_id'])



def main():
    cc = ControlConsumer(queue_name='performance_monitor')
    cc.start()
    # print('Initialized consumer listener', flush=True)
    # # Wait for the system to initialize
    # time.sleep(180)

    # print('Started consumer listener')
    
    # QUEUE_NAME = 'performance_monitor'

    # rabbitmq_credentials = pika.PlainCredentials(credentials['rabbitmq_user'], credentials['rabbitmq_password'])
    # connection = pika.BlockingConnection(pika.ConnectionParameters('rabbitmq', '5672', '/', rabbitmq_credentials)) 
    # channel = connection.channel()

    # channel.queue_declare(queue=QUEUE_NAME)

    # channel.basic_consume(queue=QUEUE_NAME, on_message_callback=callback, auto_ack=True)

    # print('[*] Waiting for messages.')
    # channel.start_consuming()


if __name__ == "__main__":

    main()
