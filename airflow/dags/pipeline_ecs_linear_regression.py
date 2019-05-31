import logging
import json

from airflow import DAG
from datetime import datetime

from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.ecs_operator import ECSOperator

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 5, 1),
}

ecs_operator_base_args = {
    'task_id': 'test_performance_task_airflow',
    'task_definition': 'test_performance_task:1',
    'cluster': 'test',
    'overrides': {},
    'aws_conn_id': 'aws-ecs-connection',
    'region_name': 'eu-west-1',
    'launch_type': 'FARGATE',
    'placement_constraints': [],
    'network_configuration': {
        'awsvpcConfiguration': {
            'assignPublicIp': 'ENABLED',
            'securityGroups': ['sg-09049336d401e3002'],
            'subnets': ['subnet-052cad2a8ac84b53a', 'subnet-092a4471612aee6d6']
        }
    }
}


def donothing():
    pass


with DAG('pipeline_ecs_linear_regression_', default_args=default_args) as dag:

    t1_additional_args = {
        'task_id': 'clean_data_airflow',
        'task_definition': 'clean_data:1'
    }
    t1_args = {**ecs_operator_base_args, **t1_additional_args}
    t1 = ECSOperator(**t1_args)

    t2_additional_args = {
        'task_id': 'adjust_data_linear_regression_airflow',
        'task_definition': 'adjust_data_linear_regression:1'
    }
    t2_args = {**ecs_operator_base_args, **t2_additional_args}
    t2 = ECSOperator(**t2_args)

    t3_additional_args = {
        'task_id': 'linear_regression_airflow',
        'task_definition': 'linear_regression:1'
    }
    t3_args = {**ecs_operator_base_args, **t3_additional_args}
    t3 = ECSOperator(**t3_args)

    t4_additional_args = {
        'task_id': 'score_linear_regression_airflow',
        'task_definition': 'score_linear_regression:1'
    }
    t4_args = {**ecs_operator_base_args, **t4_additional_args}
    t4 = ECSOperator(**t4_args)

    t1 >> t2 >> t3 >> t4
