FROM python:3.6.8-jessie

# will be overwriten should `docker run` pass a proper env
ENV EXECUTION_ID 111111

COPY run.sh ./task/run.sh

WORKDIR task
ENTRYPOINT ["bash", "./run.sh"]
