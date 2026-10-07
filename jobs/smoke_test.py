from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("smoke_test").getOrCreate()

# 1) Iceberg via Nessie, con MERGE (idempotente: correrlo dos veces no duplica)
spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.smoke")
spark.sql("CREATE TABLE IF NOT EXISTS nessie.smoke.t (id INT, name STRING) USING iceberg")
spark.createDataFrame([(1, "a"), (2, "b")], ["id", "name"]).createOrReplaceTempView("src")
spark.sql("""
    MERGE INTO nessie.smoke.t t USING src s ON t.id = s.id
    WHEN MATCHED THEN UPDATE SET t.name = s.name
    WHEN NOT MATCHED THEN INSERT *
""")
print("ICEBERG filas:", spark.table("nessie.smoke.t").count())

# 2) Parquet en MinIO (ruta fija, con sobrescritura)
spark.range(5).write.mode("overwrite").parquet("s3a://bronze/_smoke/")
print("PARQUET filas:", spark.read.parquet("s3a://bronze/_smoke/").count())

spark.stop()