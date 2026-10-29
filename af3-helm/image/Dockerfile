# Airflow 3.3.2 / Python 3.14 runtime image for the v3 stack (migration plan §5.4).
#
# One immutable image for the api-server, scheduler, dag-processor, triggerer, Celery workers
# and the migration Job. It holds no DAGs and no `orchestration` plugin code: both come from the
# Azure Files share, so a DAG release never needs an image rebuild.
#
# Build from af3-helm/image/:
#   docker build --platform linux/amd64 -t <registry>/airflow:3.3.2-python3.14-<build> .
# then scan, push, and deploy by digest (airflow.images.airflow.digest), never by tag.

# The slim variant carries core Airflow without the default provider bundle, so the image holds
# exactly the providers in requirements.af3.in (plan §5.3: remove unused providers).
# Pinned by digest; re-resolve it only together with the lock.
ARG BASE_IMAGE=apache/airflow:slim-3.3.2-python3.14@sha256:35528ff18927be9840aff403a1ec445a742ec3fbd9bca04b7a77379d2c782491
FROM ${BASE_IMAGE}

COPY --chown=airflow:0 requirements.af3.lock /opt/airflow/requirements.af3.lock

# The lock pins apache-airflow==3.3.2 itself, so this install cannot move Airflow. pip check
# fails the build if anything already in the base image disagrees with the lock.
RUN pip install --no-cache-dir -r /opt/airflow/requirements.af3.lock \
 && pip check
