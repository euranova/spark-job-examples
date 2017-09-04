#!/usr/bin/env bash

CLUSTER_NAME=${CLUSTER_NAME:-cluster}
KAFKA_VERSION=${KAFKA_VERSION:-0.10.0.0}

function init {
  echo "Creating Kafka topics..."

  topic_exists "words" || create_topic "words"
  topic_exists "messages" || create_topic "messages"
  topic_exists "trigrams" || create_topic "trigrams" "--config cleanup.policy=compact"

  echo "Topics created."
}

function create_topic {
  TOPIC=$1
  OPTIONS=$2

  echo "Creating topic $TOPIC"

  docker exec -it \
  ${CLUSTER_NAME}-kafka \
    /opt/kafka_2.11-${KAFKA_VERSION}/bin/kafka-topics.sh --create \
      --zookeeper localhost:2181  \
      --partitions 60 \
      --replication-factor 1 \
      --config cleanup.policy=compact \
      ${OPTIONS} \
      --topic ${TOPIC}
}


function topic_exists {
  TOPIC=$1

  docker exec -it \
    ${CLUSTER_NAME}-kafka \
      /opt/kafka_2.11-${KAFKA_VERSION}/bin/kafka-topics.sh \
        --zookeeper localhost:2181  \
        --list | grep "${TOPIC}" > /dev/null
  return $?
}

init



