#!/usr/bin/env bash

CLUSTER_NAME=${CLUSTER_NAME:-cluster}
KAFKA_VERSION=${KAFKA_VERSION:-0.10.0.0}

TOPIC=$1

docker exec -it \
  ${CLUSTER_NAME}-kafka \
    /opt/kafka_2.11-${KAFKA_VERSION}/bin/kafka-console-producer.sh \
      --broker-list kafka:9092 \
       --property "parse.key=true" \
       --property "key.separator=:" \
      --topic ${TOPIC}
