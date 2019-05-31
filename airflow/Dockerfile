FROM alvyl/docker-airflow:1.10.3

USER root

# linux
RUN groupadd --gid 999 docker \
    && usermod -aG docker airflow

USER airflow