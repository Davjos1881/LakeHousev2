"""Silver: posts_hist (Iceberg, MERGE por post_id) construido desde los Parquet de Bronze.

Uso (con el Python principal, no el del venv de DLT):
    python silver_posts.py                 # todos los periodos de config.PERIODS
    python silver_posts.py 2020_H1         # solo un periodo

Reglas: nombres en snake_case, textos binary -> string, ceros y vacios -> NULL,
fecha 1970 (vacio de ClickHouse) -> NULL, sin Body, deduplicacion por post_id,
filas invalidas rechazadas y contadas en silver.dq_log, columna load_date.
"""
import sys

from pyspark import StorageLevel
from pyspark.sql import Window
from pyspark.sql import functions as F

import config
import silver_lib

TABLE = silver_lib.table_name(config.SILVER_NS, "posts_hist")

VALUE_COLS = [  # columnas que entran en el hash de cambios
    "post_type_id", "accepted_answer_id", "creation_date", "score", "view_count", "owner_user_id",
    "last_activity_date", "tags", "answer_count", "comment_count", "favorite_count", "parent_id",
    "closed_date",
]
TABLE_DDL = f"""
CREATE TABLE IF NOT EXISTS {TABLE} (
  post_id BIGINT, post_type_id INT, accepted_answer_id BIGINT, creation_date TIMESTAMP,
  score INT, view_count INT, owner_user_id BIGINT, last_activity_date TIMESTAMP, tags STRING,
  answer_count INT, comment_count INT, favorite_count INT, parent_id BIGINT, closed_date TIMESTAMP,
  source_period STRING, row_hash STRING, load_date TIMESTAMP
) USING iceberg
"""
OUTPUT_COLS = ["post_id"] + VALUE_COLS + ["source_period", "row_hash", "load_date"]


def read_bronze(spark, periods):
    dfs = [spark.read.parquet(config.bronze_s3a("posts", p)).withColumn("source_period", F.lit(p))
           for p in periods]
    out = dfs[0]
    for d in dfs[1:]:
        out = out.unionByName(d)
    return out


def _zero_to_null(c):
    return F.when(F.col(c) == 0, F.lit(None)).otherwise(F.col(c))


def clean_posts(df):
    """Normaliza nombres y tipos. No descarta filas (eso lo decide la capa de calidad)."""
    tags = F.col("Tags").cast("string")
    parent = F.col("ParentId").cast("string")
    return df.select(
        F.col("Id").cast("long").alias("post_id"),
        F.col("PostTypeId").cast("int").alias("post_type_id"),
        _zero_to_null("AcceptedAnswerId").cast("long").alias("accepted_answer_id"),
        F.col("CreationDate").alias("creation_date"),
        F.col("Score").cast("int").alias("score"),
        F.col("ViewCount").cast("int").alias("view_count"),
        _zero_to_null("OwnerUserId").cast("long").alias("owner_user_id"),
        F.col("LastActivityDate").alias("last_activity_date"),
        F.when(F.length(F.trim(tags)) == 0, F.lit(None)).otherwise(tags).alias("tags"),
        F.col("AnswerCount").cast("int").alias("answer_count"),
        F.col("CommentCount").cast("int").alias("comment_count"),
        F.col("FavoriteCount").cast("int").alias("favorite_count"),
        F.when(F.length(F.trim(parent)) == 0, F.lit(None)).otherwise(parent.cast("long")).alias("parent_id"),
        F.when(F.col("ClosedDate") <= F.to_timestamp(F.lit("1970-01-02")), F.lit(None))
         .otherwise(F.col("ClosedDate")).alias("closed_date"),
        F.col("source_period"),
    )


def _valid_expr():
    return (
        F.col("post_id").isNotNull()
        & F.col("creation_date").isNotNull()
        & F.col("post_type_id").between(1, 8)
        & (F.col("creation_date") <= F.current_timestamp())
    )


def _count_if(cond):
    return F.sum(F.when(cond, 1).otherwise(0))


def quality_stats(df):
    """Una sola pasada sobre los datos. Devuelve la lista de reglas para silver.dq_log."""
    valid = _valid_expr()
    r = df.agg(
        F.count(F.lit(1)).alias("total"),
        _count_if(F.col("post_id").isNull()).alias("post_id_nulo"),
        _count_if(F.col("creation_date").isNull()).alias("creation_date_nula"),
        _count_if(F.col("post_type_id").isNull() | ~F.col("post_type_id").between(1, 8)).alias("tipo_invalido"),
        _count_if(F.col("creation_date") > F.current_timestamp()).alias("fecha_futura"),
        _count_if(valid).alias("validas"),
        F.countDistinct(F.when(valid, F.col("post_id"))).alias("ids_distintos"),
        _count_if(F.col("owner_user_id").isNull()).alias("sin_autor"),
        _count_if((F.col("post_type_id") == 2) & F.col("parent_id").isNull()).alias("respuesta_sin_padre"),
    ).first()
    total = r["total"]
    return [
        ("post_id_nulo", r["post_id_nulo"], total, "rechazada"),
        ("creation_date_nula", r["creation_date_nula"], total, "rechazada"),
        ("post_type_invalido", r["tipo_invalido"], total, "rechazada"),
        ("fecha_futura", r["fecha_futura"], total, "rechazada"),
        ("duplicados_por_post_id", r["validas"] - r["ids_distintos"], total, "deduplicada"),
        ("owner_user_id_nulo", r["sin_autor"], total, "informativa (queda NULL)"),
        ("respuesta_sin_padre", r["respuesta_sin_padre"], total, "informativa"),
    ]


def dedupe(df):
    """Un registro por post_id: el de actividad mas reciente."""
    w = Window.partitionBy("post_id").orderBy(
        F.col("last_activity_date").desc_nulls_last(), F.col("source_period").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def with_hash_and_load_date(df):
    parts = [F.coalesce(F.col(c).cast("string"), F.lit("<null>")) for c in VALUE_COLS]
    return (df.withColumn("row_hash", F.sha2(F.concat_ws("||", *parts), 256))
              .withColumn("load_date", F.current_timestamp())
              .select(*OUTPUT_COLS))


def changed_rows(df, target):
    """Solo lo NUEVO o MODIFICADO: filas de origen cuyo (post_id, row_hash) no esta en el destino.
    Compara dos columnas, asi que es liviano. Si nada cambio, devuelve 0 filas."""
    return df.join(target.select("post_id", "row_hash"), ["post_id", "row_hash"], "left_anti")


def merge(spark, df):
    """MERGE por post_id. Recibe solo filas nuevas o modificadas (ver changed_rows): asi Iceberg
    no reescribe archivos que no cambiaron y repetir el proceso no deja archivos nuevos."""
    df.createOrReplaceTempView("posts_src")
    spark.sql(f"""
        MERGE INTO {TABLE} t USING posts_src s ON t.post_id = s.post_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)


def run(periods):
    spark = silver_lib.get_spark("silver_posts")
    silver_lib.ensure_namespaces(spark)
    spark.sql(TABLE_DDL)
    cleaned = clean_posts(read_bronze(spark, periods))
    checks = quality_stats(cleaned)
    valid = dedupe(cleaned.filter(_valid_expr()))

    delta = changed_rows(with_hash_and_load_date(valid), spark.table(TABLE))
    delta = delta.persist(StorageLevel.DISK_ONLY)  # se calcula una vez; a disco para no usar memoria
    n_delta = delta.count()
    print(f"  filas nuevas o modificadas: {n_delta}")

    if n_delta:
        merge(spark, delta)
    else:
        print("  sin cambios: no se escribe nada en la tabla")
    delta.unpersist()

    silver_lib.log_dq(spark, TABLE, checks)
    n = spark.table(TABLE).count()
    print(f"OK {TABLE}: {n} filas")
    spark.stop()
    return n


if __name__ == "__main__":
    periods = sys.argv[1:] or sorted(config.PERIODS)
    run(periods)