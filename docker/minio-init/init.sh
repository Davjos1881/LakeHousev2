#!/bin/sh
# Idempotente: si el bucket existe, no hace nada ni crea carpetas nuevas.
set -eu
EP=http://minio:9000
aws configure set default.s3.addressing_style path

i=0
until aws --endpoint-url "$EP" s3api list-buckets >/dev/null 2>&1; do
  i=$((i+1))
  if [ "$i" -ge 30 ]; then echo "MinIO no responde"; exit 1; fi
  sleep 2
done

for b in bronze warehouse; do
  aws --endpoint-url "$EP" s3api head-bucket --bucket "$b" 2>/dev/null \
    || aws --endpoint-url "$EP" s3 mb "s3://$b"
done
echo "Buckets listos:"
aws --endpoint-url "$EP" s3 ls
