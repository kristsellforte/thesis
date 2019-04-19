import logging
import docker
import tarfile
import json
import os
import tempfile
import pickle

from airflow.models import TaskInstance
from docker import Client
from docker.errors import NotFound
from sklearn import linear_model

log = logging.getLogger(__name__)


def combine_xcom_values(xcoms):
    if xcoms is None or xcoms == [] or xcoms == () or xcoms == (None, ):
        return {}
    elif len(xcoms) == 1:
        return dict(xcoms)

    result = {}
    egible_xcoms = (d for d in xcoms if d is not None and len(d) > 0)
    for d in egible_xcoms:
        for k, v in d.items():
            result[k] = v
    return result


def untar_file_and_get_result_json(client, container):
    try:
        tar_data_stream, stat = client.get_archive(container=container, path='/tmp/result.tgz')
    except NotFound:
        return dict()

    with tempfile.NamedTemporaryFile() as tmp:
        for chunk in tar_data_stream.stream():
            tmp.write(chunk)
        tmp.seek(0)
        with tarfile.open(mode='r', fileobj=tmp) as tar:
            tar.extractall()
            tar.close()

    with tarfile.open('result.tgz') as tf:
        for member in tf.getmembers():
            f = tf.extractfile(member)
            log.info(member.name)
            if member.name.find(".pkl") != -1:
                # treat as pickle
                log.info(f)
                result = pickle.load(f)
            elif member.name.find(".json") != -1:
                # treat as json
                result = json.loads(f.read())
            else:
                result = {}
            os.remove('result.tgz')
            return result


def pull_all_parent_xcoms(context):
    pulled_data = {}
    for task_id in context['task'].upstream_task_ids:
        pulled_data[task_id] = context['task_instance'].xcom_pull(task_ids=task_id, key='data')

    return pulled_data


def launch_docker_container(**context):
    image_name = context['image_name']
    client: Client = docker.from_env()

    log.info(f"Creating image {image_name}")

    execution_id = context['dag_run'].run_id

    command = ''

    args_json = pull_all_parent_xcoms(context)

    environment = {
        'EXECUTION_ID': execution_id
    }

    # centralize storage with volumes
    volumes= ['/data', '/models', '/scores']
    volume_bindings = {
                        '/Users/kristskreics/code/thesis2/thesis/airflow/data': {
                            'bind': '/data',
                            'mode': 'rw'
                        },
                        '/Users/kristskreics/code/thesis2/thesis/airflow/models': {
                            'bind': '/models',
                            'mode': 'rw'
                        },
                        '/Users/kristskreics/code/thesis2/thesis/airflow/scores': {
                            'bind': '/scores',
                            'mode': 'rw'
                        },
                        '/Users/kristskreics/code/thesis2/thesis/airflow/config': {
                            'bind': '/config',
                            'mode': 'rw'
                        }
    }

    host_config = client.create_host_config(binds=volume_bindings)

    container = client.create_container(image=image_name, environment=environment, command=command, volumes=volumes, host_config=host_config)

    container_id = container.get('Id')
    log.info(f"Running container with id {container_id}")
    client.start(container=container_id)

    logs = client.logs(container_id, follow=True, stderr=True, stdout=True, stream=True, tail='all')

    try:
        while True:
            l = next(logs)
            log.info(f"Task log: {l}")
    except StopIteration:
        log.info("Docker has finished!")

    result = untar_file_and_get_result_json(client, container)
    log.info(f"Result was {result}")
    context['task_instance'].xcom_push('data', result, context['execution_date'])