#!/usr/bin/env bash

# RUN THE JOB ON THE CLUSTER

docker run -it --rm --network cluster_default \
    -v $PWD:/code -w /code gettyimages/spark \
    /usr/spark-2.1.0/bin/spark-submit \
    /code/target/spark-sandbox-1.0-SNAPSHOT.jar

# RUN THE JOB ON THE CLUSTER WITH YARN

#cat target/spark-sandbox-1.0-SNAPSHOT.jar | docker exec -i  cluster-hdfs-namenode hdfs dfs -put - /job.jar

docker run -it --rm --network cluster_default \
    -e YARN_CONF_DIR=/code/hadoop-conf \
    -v $PWD:/code -w /code gettyimages/spark \
    /usr/spark-2.1.0/bin/spark-submit \
    --master yarn \
    --deploy-mode cluster \
    /code/target/spark-sandbox-1.0-SNAPSHOT.jar

# hdfs://cluster-hdfs-namenode/job.jar

# CREATE A TOPIC ON KAFKA
docker exec -it \
  cluster-kafka \
    /opt/kafka_2.11-0.10.0.0/bin/kafka-topics.sh --create \
      --zookeeper localhost:2181  \
      --partitions 60 \
      --replication-factor 1 \
      --topic messages

docker exec -it \
  cluster-kafka \
    /opt/kafka_2.11-0.10.0.0/bin/kafka-topics.sh --create \
      --zookeeper localhost:2181  \
      --partitions 60 \
      --replication-factor 1 \
      --config cleanup.policy=compact \
      --topic trigrams

# SET TOPIC AS COMPACT
#docker exec -it   cluster-kafka \
#    /opt/kafka_2.11-0.10.0.0/bin/kafka-configs.sh  \
#    --alter \
#    --zookeeper localhost:2181 \
#    --add-config cleanup.policy=compact \
#    --entity-type topics \
#    --entity-name trigrams


# LISTEN TO A TOPIC ON KAFKA
docker exec -it   \
    cluster-kafka \
    /opt/kafka_2.11-0.10.0.0/bin/kafka-console-consumer.sh \
    --bootstrap-server kafka:9092 \
    --zookeeper 127.0.0.1:2181 \
    --topic messages

# PRODUCE TO A TOPIC ON KAFKA

docker exec -it \
  cluster-kafka \
      /opt/kafka_2.11-0.10.0.0/bin/kafka-console-producer.sh \
      --broker-list kafka:9092 \
       --property "parse.key=true" \
       --property "key.separator=:" \
      --topic messages

docker exec -it \
  cluster-kafka \
      /opt/kafka_2.11-0.10.0.0/bin/kafka-console-producer.sh \
      --broker-list kafka:9092 \
       --property "parse.key=true" \
       --property "key.separator=:" \
      --topic trigrams


#To upload the file in HDFS :

docker exec -it cluster-hdfs-namenode hdfs dfs -put /external_data/All_Geographies.data /


# Run job:
docker run --rm --name job-runner --network cluster_default -v $PWD:/workspace -w /workspace openjdk:8-jre-alpine java -jar ./target/spark-sandbox-1.0-SNAPSHOT.jar


# dirs & permissions
docker exec -it cluster-hdfs-namenode hdfs dfs -mkdir /sparkstreamingcheckpoint
docker exec -it cluster-hdfs-namenode hdfs dfs -chown jehan:jehan /sparkstreamingcheckpoint



#### messages

# swift events

#QDU:I'm the boss
#JBR:I think I'm not the boss
#JBR:Oh no
#QDU:No what ? Is C-SaM late again ?
#JBR:Dunno. Ask ADE.
#ADE:Leave me alone !

# swift bics

#JBR:Jehan Bruggeman
#QDU:Quentin Dugauthier
#ADE:Alexandre D'Erman

