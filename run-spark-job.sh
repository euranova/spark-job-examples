#!/usr/bin/env bash

cd $(dirname $0)

CLUSTER_NAME=cluster
PROJECT_NAME=spark-jobs
JOB_NAME=${1:-BatchSimple}
MASTER=${2:-spark://${CLUSTER_NAME}-spark-master:7077}
CONTAINER_NAME=${CLUSTER_NAME}-job-runner


docker rm ${CONTAINER_NAME} > /dev/null 2>&1

docker run --rm --name ${CONTAINER_NAME} \
  --network ${CLUSTER_NAME}_default \
  -v $PWD:/workspace \
  -w /workspace/ \
  gettyimages/spark:2.1.0-hadoop-2.7 \
  /usr/spark-2.1.0/bin/spark-submit \
  --master ${MASTER} \
  --properties-file spark-jobs-config.conf \
  --jars ./${PROJECT_NAME}/build/libs/${PROJECT_NAME}-1.0-SNAPSHOT-all.jar \
  --class sparksandbox.${JOB_NAME} \
  ./${PROJECT_NAME}/build/libs/${PROJECT_NAME}-1.0-SNAPSHOT.jar
