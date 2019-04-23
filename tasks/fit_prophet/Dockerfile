FROM python:3.6.8-jessie

COPY requirements.txt /

# will be overwriten should `docker run` pass a proper env
ENV EXECUTION_ID 111111

# they HAVE to match the name of jupyter's kernel
RUN apt-get update
RUN apt-get -y upgrade
RUN apt-get install build-essential
RUN apt-get install -y python3-dev
RUN pip install virtualenv
RUN virtualenv -p python3 airflow_jupyter
RUN /bin/bash -c "source /airflow_jupyter/bin/activate"
RUN pip install pystan==2.17.1.0
RUN pip install -r /requirements.txt

RUN mkdir main

COPY code.py ./main/code.py

WORKDIR main
ENTRYPOINT ["python", "code.py"]
