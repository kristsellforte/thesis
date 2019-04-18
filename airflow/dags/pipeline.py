import logging

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

from airflow.operators.python_operator import PythonOperator

from launcher.launcher import launch_docker_container
from launcher.launcher_docker import do_test_docker

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2019, 2, 15),
}

def donothing():
    pass

def read_xcoms(**context):
    for idx, task_id in enumerate(context['data_to_read']):
        data = context['task_instance'].xcom_pull(task_ids=task_id, key='data')
        logging.info(f'[{idx}] I have received data: {data} from task {task_id}')


with DAG('pipeline', default_args=default_args) as dag:

    t1 = PythonOperator(
        task_id="test_docker",
        python_callable=do_test_docker
    )

    t2_id = 'linear_regression'
    t2 = PythonOperator(
        task_id=t2_id,
        provide_context=True,
        op_kwargs={
            'image_name': 'linear_regression'
        },
        python_callable=launch_docker_container
    )

    t3_id = 'score_linear_regression'
    t3 = PythonOperator(
        task_id=t3_id,
        provide_context=True,
        op_kwargs={
            'image_name': 'score_linear_regression'
        },
        python_callable=launch_docker_container
    )

    t4_id = 'adjust_data'
    t4 = PythonOperator(
        task_id=t4_id,
        provide_context=True,
        op_kwargs={
            'image_name': 'data_adjustment'
        },
        python_callable=launch_docker_container
    )

    # t4 = PythonOperator(
    #     task_id='read_xcoms',
    #     provide_context=True,
    #     python_callable=read_xcoms,
    #     op_kwargs={
    #         'data_to_read': [t2_id, t3_id]
    #     }
    # )

    t1 >> t4 >> t2 >> t3
