#!/usr/bin/env bash

ROOT=$(dirname $0)


cd $ROOT/cluster


docker-compose up -d || { echo "Failed to initialize cluster"; exit 1; }

./hadoop/init_data.sh || { echo "Failed to load data into HDFS"; exit 1; }

./kafka/kafka_init.sh || { echo "Failed to create Kafka topics"; exit 1; }
