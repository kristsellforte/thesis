FROM python:3.6.8-jessie

COPY requirements.txt /

# will be overwriten should `docker run` pass a proper env
ENV EXECUTION_ID 111111

# they HAVE to match the name of jupyter's kernel
RUN pip install virtualenv
RUN virtualenv -p python3 airflow_jupyter
RUN /bin/bash -c "source /airflow_jupyter/bin/activate"
RUN pip install -r /requirements.txt

RUN mkdir main

COPY ./models ./main/models
COPY data.csv ./main/data.csv
COPY scores.json ./main/scores.json
COPY code.py ./main/code.py

WORKDIR main
ENTRYPOINT ["python", "code.py"]
