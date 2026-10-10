"""Utilidades compartidas de Silver y Gold: sesion Spark, namespaces y registro de calidad de datos."""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

import config


def get_spark(app_name):
    """La configuracion (cluster, catalogo Nessie, JARs, S3) viene de spark-defaults.conf."""
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    # Lectura de Iceberg SIN Arrow/Netty: la ruta vectorizada provoca SIGSEGV en los executors
    # (Java 17 en este entorno). Es mas lenta, pero estable y reproducible. Se puede habilitar en entornos donde no haya problemas.
    spark.conf.set("spark.sql.iceberg.vectorization.enabled", "false")
    return spark


def table_name(namespace, name):
    return f"{config.CATALOG}.{namespace}.{name}"


def ensure_namespaces(spark):
    for ns in (config.SILVER_NS, config.GOLD_NS):
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {config.CATALOG}.{ns}")


DQ_TABLE = table_name(config.SILVER_NS, "dq_log")


def log_dq(spark, target_table, checks):
    """Registra el resultado de las reglas de calidad. Es un UPSERT por (tabla, regla):
    guarda el estado de la ultima ejecucion y no crece al repetir el pipeline.
    checks: lista de (regla, filas_afectadas, filas_totales, accion)."""
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {DQ_TABLE} (table_name STRING, check_name STRING, "
        "failed_rows BIGINT, total_rows BIGINT, action STRING, load_date TIMESTAMP) USING iceberg"
    )
    rows = [(target_table, c, int(f), int(t), a) for c, f, t, a in checks]
    (spark.createDataFrame(rows, "table_name string, check_name string, failed_rows long, "
                                 "total_rows long, action string")
          .withColumn("load_date", F.current_timestamp())
          .createOrReplaceTempView("dq_src"))
    spark.sql(
        f"MERGE INTO {DQ_TABLE} t USING dq_src s "
        "ON t.table_name = s.table_name AND t.check_name = s.check_name "
        "WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *"
    )
    for c, f, t, a in checks:
        print(f"  DQ {target_table:<28} {c:<24} {f:>10} / {t:<10} {a}")