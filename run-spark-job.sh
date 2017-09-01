#!/usr/bin/env bash

cd $(dirname $0)

PROJECT_NAME=cluster

#
#docker run --rm --name ${PREFIX}-job-runner \
#  --network ${PREFIX}_default \
#  -v $PWD/spark-job:/workspace -w /workspace/ \
#  openjdk:8-jre-alpine \
#  java -jar ./build/libs/spark-job-1.0-SNAPSHOT-all.jar
#

docker run --rm --name ${PROJECT_NAME}-job-runner \
  --network ${PROJECT_NAME}_default \
  -v $PWD/spark-job:/workspace \
  -w /workspace/ \
  gettyimages/spark:2.1.0-hadoop-2.7 \
  /usr/spark-2.1.0/bin/spark-submit \
  --master "spark://${PROJECT_NAME}-spark-master:7077" \
  --jars ./build/libs/spark-cluster-sandbox-1.0-SNAPSHOT-all.jar \
  --class sparksandbox.UnStructuredStreaming \
  ./build/libs/spark-cluster-sandbox-1.0-SNAPSHOT.jar


  #-v $PWD/spark-ivy2-cache:/root/.ivy2/ \
  # --jars ./build/libs/spark-job-1.0-SNAPSHOT-all.jar \
#  --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.1.0,com.databricks:spark-avro_2.11:3.2.0 \


exit

docker run --rm --name ${PROJECT_NAME}-job-runner \
  --network ${PROJECT_NAME}_default \
  -v $PWD:/workspace \
  -w /workspace/ \
  gettyimages/spark:2.1.0-hadoop-2.7 \
  /usr/spark-2.1.0/bin/spark-submit \
  --properties-file  spark-jobs-config.conf \
  --jars ./spark-jobs/build/libs/spark-jobs-1.0-SNAPSHOT-all.jar \
  --class sparksandbox.BatchSimple \
  ./spark-jobs/build/libs/spark-jobs-1.0-SNAPSHOT.jar

