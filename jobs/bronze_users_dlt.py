"""Bronze via DLT: ingesta de `users` de un periodo (la parte que corre en Airflow).

Uso (con el Python del venv de DLT, variable DLT_PYTHON en el contenedor de Airflow):
    $DLT_PYTHON bronze_users_dlt.py 2021_H1

Escribe en s3://bronze/users/<periodo>/users_<periodo>.parquet (ruta FIJA, sobrescribe), con las
mismas columnas y tipos que el seed manual. Usuarios = autores de los posts del periodo (opcion B),
asi que los posts de Bronze del mismo periodo deben existir antes.

Sin carpetas nuevas por ejecucion:
- la ruta de datos depende solo de dataset y periodo (write_disposition="replace");
- _dlt_loads crece un archivo por ejecucion -> se poda dejando solo el mas reciente;
- el estado local de DLT se borra al terminar (se restaura desde el destino si hace falta).
"""
import os

# Deben fijarse ANTES de importar dlt
os.environ.setdefault("SCHEMA__NAMING", "direct")            # conserva nombres de columnas/tablas tal cual
os.environ.setdefault("RUNTIME__DLTHUB_TELEMETRY", "false")  # sin llamadas a internet de telemetria

import shutil
import sys
import time

import dlt
import pyarrow.compute as pc
import pyarrow.parquet as pq

import config
from bronze_seed import author_ids, fs_minio, fs_source, stream_filtered

DATASET = "users"
PIPELINE = "bronze_users"
PIPELINES_DIR = os.getenv("DLT_PIPELINES_DIR", "/tmp/dlt_pipelines")


def users_batches(period, src, dst):
    ids = author_ids(dst, period)
    src_path = f"{config.SOURCE_BASE}/users.parquet"
    print(f"users {period}: {len(ids)} autores distintos; {src_path}", flush=True)
    yield from stream_filtered(src, src_path, lambda t: t.filter(pc.is_in(t["Id"], value_set=ids)))


def make_destination(bucket_url=None, credentials=None):
    if bucket_url is None:  # MinIO
        bucket_url = f"s3://{config.BRONZE_BUCKET}"
        credentials = {
            "aws_access_key_id": config.MINIO_KEY,
            "aws_secret_access_key": config.MINIO_SECRET,
            "endpoint_url": config.MINIO_ENDPOINT,
            "region_name": os.getenv("AWS_REGION", "us-east-1"),
        }
    kwargs = {"credentials": credentials} if credentials else {}
    # {table_name} es el periodo (por eso la ruta queda users/<periodo>/users_<periodo>.parquet)
    return dlt.destinations.filesystem(
        bucket_url=bucket_url, layout="{table_name}/users_{table_name}.{ext}", **kwargs)


def prune_loads(dst):
    """_dlt_loads guarda un archivo por ejecucion: dejamos solo el mas reciente."""
    files = dst.glob(f"{config.BRONZE_BUCKET}/{DATASET}/_dlt_loads/*.jsonl")
    load_id = lambda p: float(p.rsplit("__", 1)[1][: -len(".jsonl")])
    for p in sorted(files, key=load_id)[:-1]:
        dst.rm(p)


def run(period, src=None, dst=None, bucket_url=None, credentials=None):
    config.period_window(period)  # valida el periodo
    src, dst = src or fs_source(), dst or fs_minio()
    dst.ls(config.BRONZE_BUCKET)  # falla en segundos si MinIO no es alcanzable
    resource = dlt.resource(users_batches(period, src, dst), name=period, write_disposition="replace")
    pipeline = dlt.pipeline(pipeline_name=PIPELINE, destination=make_destination(bucket_url, credentials),
                            dataset_name=DATASET, pipelines_dir=PIPELINES_DIR)
    try:
        pipeline.run(resource, loader_file_format="parquet")
        prune_loads(dst)
    finally:
        shutil.rmtree(os.path.join(PIPELINES_DIR, PIPELINE), ignore_errors=True)
    path = config.bronze_path(DATASET, period)
    with dst.open(path, "rb") as f:
        rows = pq.ParquetFile(f).metadata.num_rows
    return path, rows, dst.size(path) / 1e6


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Uso: python bronze_users_dlt.py <periodo>   (periodos: " + ", ".join(sorted(config.PERIODS)) + ")")
    t0 = time.time()
    path, rows, mb = run(sys.argv[1])
    print(f"LISTO: {rows} filas, {mb:.1f} MB en s3://{path} ({time.time() - t0:.0f}s)")