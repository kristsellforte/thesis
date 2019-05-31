import logging
import json

from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ecs_operator import ECSOperator
# from launcher.launcher import launch_docker_container
# from launcher.launcher_docker import do_test_docker

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 15),
}

def donothing():
    pass


# def read_xcoms(**context):
#     for idx, task_id in enumerate(context['data_to_read']):
#         data = context['task_instance'].xcom_pull(task_ids=task_id, key='data')
#         logging.info(f'[{idx}] I have received data: {data} from task {task_id}')

# quality_setting = get_quality_setting()

with DAG('pipeline_ecs', default_args=default_args) as dag:
    ecs_operator_args = {
        'task_id': 'test_performance_task_airflow',
        'task_definition': 'test_performance_task:1',
        'cluster': 'test',
        'overrides': {},
        'aws_conn_id': 'aws-ecs-connection',
        'region_name': 'eu-west-1',
        'launch_type': 'FARGATE',
        # 'group': 'service:test-task-service',
        'placement_constraints': [],
        'network_configuration': {
            'awsvpcConfiguration': {
                'assignPublicIp': 'ENABLED',
                'securityGroups': ['sg-09049336d401e3002'],
                'subnets': ['subnet-052cad2a8ac84b53a', 'subnet-092a4471612aee6d6']
            }
        }
    }

    # t1_id = 'test-task'
    t1 = ECSOperator(**ecs_operator_args)

    # t2_id = 'adjust_data_linear_regression'
    # t2 = PythonOperator(
    #     task_id=t2_id,
    #     provide_context=True,
    #     op_kwargs={
    #         'image_name': t2_id
    #     },
    #     python_callable=launch_docker_container
    # )

    # t3_id = 'linear_regression'
    # t3 = PythonOperator(
    #     task_id=t3_id,
    #     provide_context=True,
    #     op_kwargs={
    #         'image_name': t3_id
    #     },
    #     python_callable=launch_docker_container
    # )

    # t4_id = 'score_linear_regression'
    # t4 = PythonOperator(
    #     task_id=t4_id,
    #     provide_context=True,
    #     op_kwargs={
    #         'image_name': t4_id
    #     },
    #     python_callable=launch_docker_container
    # )

    t1
