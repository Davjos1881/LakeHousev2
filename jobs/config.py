"""Configuracion central del pipeline. Unica fuente de verdad de rutas, buckets y periodos.

Solo usa la libreria estandar, para poder importarse desde cualquier entorno
(Python de Airflow/Jupyter, venv de DLT, notebooks).
"""
import os
from datetime import datetime, timezone

# ---------- MinIO (credenciales vienen de las variables del contenedor, definidas en .env) ----------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_KEY = os.getenv("AWS_ACCESS_KEY_ID")
MINIO_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY")

BRONZE_BUCKET = os.getenv("BRONZE_BUCKET", "bronze")
WAREHOUSE_BUCKET = os.getenv("WAREHOUSE_BUCKET", "warehouse")

# ---------- Fuente: dataset StackOverflow publicado por ClickHouse ----------
SOURCE_BASE = "datasets-documentation/stackoverflow/parquet"
SOURCE_REGION = "eu-west-3"

# ---------- Periodos: ene-jun de dos anios distintos ----------
def _utc(year, month, day):
    return datetime(year, month, day, tzinfo=timezone.utc)


PERIODS = {
    "2020_H1": (_utc(2020, 1, 1), _utc(2020, 7, 1)),
    "2021_H1": (_utc(2021, 1, 1), _utc(2021, 7, 1)),
}


def period_window(period):
    """Devuelve (inicio, fin_exclusivo) del periodo."""
    if period not in PERIODS:
        raise ValueError(f"Periodo desconocido: {period}. Validos: {sorted(PERIODS)}")
    return PERIODS[period]


# ---------- Rutas de Bronze: dependen SOLO de dataset y periodo (nunca de la fecha de ejecucion) ----------
def bronze_path(dataset, period):
    """Ruta sin esquema, p. ej. bronze/posts/2020_H1/posts_2020_H1.parquet"""
    return f"{BRONZE_BUCKET}/{dataset}/{period}/{dataset}_{period}.parquet"


def bronze_s3a(dataset, period):
    """URI para Spark (S3A)."""
    return f"s3a://{bronze_path(dataset, period)}"


# ---------- Silver / Gold (Iceberg via Nessie) ----------
CATALOG = "nessie"
SILVER_NS = "silver"
GOLD_NS = "gold"
