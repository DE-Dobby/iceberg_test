#!/bin/bash
set -e

echo "======================================"
echo "  Spark 3.4 MinIO Test - Starting"
echo "======================================"
echo "  MINIO_ENDPOINT  : ${MINIO_ENDPOINT:-http://minio:9000}"
echo "  MINIO_BUCKET    : ${MINIO_BUCKET:-spark-test}"
echo "======================================"

exec /opt/spark/bin/spark-submit \
  --class com.example.MinIOSparkTest \
  --master local[*] \
  --conf "spark.hadoop.fs.s3a.endpoint=${MINIO_ENDPOINT:-http://minio:9000}" \
  --conf "spark.hadoop.fs.s3a.access.key=${MINIO_ACCESS_KEY:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.secret.key=${MINIO_SECRET_KEY:-minioadmin}" \
  --conf "spark.hadoop.fs.s3a.path.style.access=true" \
  --conf "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem" \
  --conf "spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider" \
  --conf "spark.sql.shuffle.partitions=4" \
  /opt/spark/jars/spark-minio-test.jar
