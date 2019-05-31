FROM python:3.6.8-jessie

COPY requirements.txt /

# will be overwriten should `docker run` pass a proper env
ENV EXECUTION_ID 111111

# they HAVE to match the name of jupyter's kernel
RUN pip install virtualenv
RUN virtualenv -p python3 airflow_jupyter
RUN /bin/bash -c "source /airflow_jupyter/bin/activate"
RUN pip install -r /requirements.txt
RUN ipython kernel install --user --name=airflow_jupyter

RUN mkdir main

COPY code.py ./main/code.py
COPY performance_monitor.py ./main/performance_monitor.py
COPY store.py ./main/store.py
COPY credentials.py ./main/credentials.py

WORKDIR main
ENTRYPOINT ["python", "code.py"]
