FROM python:3.6-slim-buster

# Never prompt the user for choices on installation/configuration of packages
ENV DEBIAN_FRONTEND noninteractive
ENV TERM linux

ARG HOME=/usr/local/
ENV HOME=${HOME}

# Airflow
ARG AIRFLOW_VERSION=1.10.9
ARG AIRFLOW_USER_HOME=/usr/local/airflow
ENV AIRFLOW_HOME=${AIRFLOW_USER_HOME}

# Define en_US.
ENV LANGUAGE en_US.UTF-8
ENV LANG en_US.UTF-8
ENV LC_CTYPE en_US.UTF-8
ENV LC_MESSAGES en_US.UTF-8

RUN apt-get update && \
    apt-get install -y \
      netcat \
      default-libmysqlclient-dev \
      python3-pip \
      curl

# install airflow and operator dependencies
RUN useradd --shell /bin/bash --create-home --home $AIRFLOW_HOME airflow \
    && pip install --upgrade pip \
    && pip install werkzeug==0.16.0 \
    && pip install docutils==0.15 \
    && pip install boto3 \
    && pip install tweepy \
    && pip install textblob \
    && pip install beautifulsoup4 \
    && pip install emoji \
    && pip install apache-airflow[aws,celery,crypto,jdbc,mysql]==$AIRFLOW_VERSION

ENV PATH="${HOME}:${PATH}"

# Add entry point
COPY ./airflow/entrypoint.sh ${AIRFLOW_HOME}/entrypoint.sh
COPY airflow/config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY ./airflow/scripts ${AIRFLOW_HOME}/scripts

RUN chmod +x ${AIRFLOW_HOME}/entrypoint.sh
RUN chmod +x ${AIRFLOW_HOME}/scripts/etl/*.py
RUN chmod +x ${AIRFLOW_HOME}/scripts/nlp/*.py
RUN chown -R airflow ${AIRFLOW_HOME}

EXPOSE 8080 3306

USER airflow
WORKDIR ${AIRFLOW_HOME}
ENTRYPOINT ["/usr/local/airflow/entrypoint.sh"]
# set default arg for entrypoint
CMD ["webserver"]
