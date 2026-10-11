"""DAG del Lakehouse: Bronze (DLT) -> Silver (Spark) -> Gold (Spark). Se ejecuta completo con un solo Play.

La logica NO esta aqui: vive en los tres notebooks (bronze_ingest, silver_transform, gold_agg). La primera
tarea los convierte a .py con nbconvert y las demas ejecutan esos .py, asi el DAG siempre corre lo que hay
en los notebooks y no hay copias que se desactualicen. Cada notebook tambien se puede ejecutar a mano en
Jupyter si Airflow falla.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

NOTEBOOKS_DIR = "/opt/airflow/notebooks"       # ./notebooks montado en el contenedor de Airflow
EXPORT_DIR = "/tmp/notebooks_py"               # ruta fija: se sobrescribe en cada ejecucion
NBCONVERT = "/home/airflow/jupyter-venv/bin/jupyter nbconvert"  # instalado en el build de la imagen (--to python siempre escribe .py)

default_args = {
    "owner": "lakehouse",
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="lakehouse_pipeline",
    description="Bronze users 2021 (DLT) -> Silver users_hist -> Gold post_counts_by_user",
    schedule=None,                  # solo manual: un Trigger ejecuta todo
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    is_paused_upon_creation=False,
    default_args=default_args,
    tags=["lakehouse", "medallion"],
) as dag:

    # 0) Notebooks -> scripts .py (los notebooks son la unica fuente del codigo)
    export_notebooks = BashOperator(
        task_id="export_notebooks",
        bash_command=(
            f"mkdir -p {EXPORT_DIR} && "
            f"{NBCONVERT} --to python --output-dir {EXPORT_DIR} "
            f"{NOTEBOOKS_DIR}/bronze_ingest.ipynb "
            f"{NOTEBOOKS_DIR}/silver_transform.ipynb "
            f"{NOTEBOOKS_DIR}/gold_agg.ipynb"
        ),
    )

    # 1) Ingesta Bronze con DLT (no es un trabajo de Spark: lanza el venv de DLT)
    bronze_users_2021 = BashOperator(
        task_id="bronze_users_2021",
        bash_command=f"python {EXPORT_DIR}/bronze_ingest.py",
        execution_timeout=timedelta(hours=1),
    )

    # 2) Silver: users_hist (PySpark + Iceberg, MERGE)
    silver_users_hist = SparkSubmitOperator(
        task_id="silver_users_hist",
        conn_id="spark_default",
        application=f"{EXPORT_DIR}/silver_transform.py",
        name="silver_transform",
        execution_timeout=timedelta(hours=1),
    )

    # 3) Gold: post_counts_by_user (PySpark + Iceberg, MERGE)
    gold_post_counts_by_user = SparkSubmitOperator(
        task_id="gold_post_counts_by_user",
        conn_id="spark_default",
        application=f"{EXPORT_DIR}/gold_agg.py",
        name="gold_agg",
        execution_timeout=timedelta(hours=1),
    )

    export_notebooks >> bronze_users_2021 >> silver_users_hist >> gold_post_counts_by_user
