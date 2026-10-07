# UNICA imagen propia con Python/Java: la usan Airflow (webserver, scheduler, init) y Jupyter.
# Spark master/worker usan la imagen oficial apache/spark; los JARs vienen del volumen spark-jars.
FROM apache/airflow:2.9.3-python3.10

ARG AIRFLOW_VERSION=2.9.3
ARG PYTHON_VERSION=3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-17-jre-headless procps curl ca-certificates && \
    rm -rf /var/lib/apt/lists/* && \
    ln -s "$(dirname "$(dirname "$(readlink -f "$(which java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    SPARK_HOME=/home/airflow/.local/lib/python${PYTHON_VERSION}/site-packages/pyspark \
    PYSPARK_PYTHON=python3 \
    DLT_PYTHON=/home/airflow/dlt-venv/bin/python
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow
COPY --chown=airflow:root docker/lakehouse/requirements-lakehouse.txt \
                          docker/lakehouse/requirements-jupyter.txt \
                          docker/common/requirements-dlt.txt /tmp/req/

# 1) PySpark 3.5.5 (una sola descarga), con Airflow fijado
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/req/requirements-lakehouse.txt

# 2) JupyterLab en un venv que ve PySpark pero no modifica el entorno de Airflow.
#    El ultimo comando falla el build si algo no cuadra.
RUN python -m venv --system-site-packages /home/airflow/jupyter-venv && \
    python -c "import site; print(site.getusersitepackages())" \
      > /home/airflow/jupyter-venv/lib/python${PYTHON_VERSION}/site-packages/airflow-user-site.pth && \
    /home/airflow/jupyter-venv/bin/pip install --no-cache-dir -r /tmp/req/requirements-jupyter.txt && \
    /home/airflow/jupyter-venv/bin/python -c "import pyspark, jupyterlab; print('jupyter-venv ok')"

# 3) DLT aislado en su propio venv
RUN python -m venv /home/airflow/dlt-venv && \
    /home/airflow/dlt-venv/bin/pip install --no-cache-dir -r /tmp/req/requirements-dlt.txt

# 4) Configuracion de Spark (cluster + catalogo); los JARs se montan desde el volumen
COPY --chown=airflow:root docker/common/spark-defaults.conf ${SPARK_HOME}/conf/spark-defaults.conf
