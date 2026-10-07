#!/bin/sh
# Descarga los JARs de Iceberg, Nessie y S3 en BUILD TIME.
# Uso: download-jars.sh <directorio_destino>
set -eu
DEST="${1:?Uso: download-jars.sh <directorio_destino>}"
MVN=https://repo1.maven.org/maven2
ICEBERG=1.5.0
NESSIE=0.77.1
HADOOP=3.3.4
AWS_V1=1.12.262

fetch() {
  curl -fsSL --retry 5 --retry-delay 3 -o "$DEST/$(basename "$1")" "$1"
}

fetch "$MVN/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/$ICEBERG/iceberg-spark-runtime-3.5_2.12-$ICEBERG.jar"
fetch "$MVN/org/apache/iceberg/iceberg-aws-bundle/$ICEBERG/iceberg-aws-bundle-$ICEBERG.jar"
fetch "$MVN/org/projectnessie/nessie-integrations/nessie-spark-extensions-3.5_2.12/$NESSIE/nessie-spark-extensions-3.5_2.12-$NESSIE.jar"
fetch "$MVN/org/apache/hadoop/hadoop-aws/$HADOOP/hadoop-aws-$HADOOP.jar"
fetch "$MVN/com/amazonaws/aws-java-sdk-bundle/$AWS_V1/aws-java-sdk-bundle-$AWS_V1.jar"
