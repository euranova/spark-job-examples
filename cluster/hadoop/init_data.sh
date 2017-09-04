#!/usr/bin/env bash

cd $(dirname $0)
ROOT=$PWD

CLUSTER_NAME=cluster
NAMENODE_SERVICE=hdfs-namenode
NAMENODE=${CLUSTER_NAME}-${NAMENODE_SERVICE}

DATASET_URL=https://www.eea.europa.eu/data-and-maps/data/vans-8/copy_of_monitoring-of-co2-emissions-vans-2016-provisional/co2_vans_v9_csv.zip/download
DATASET_FILE_ZIP=co2-emissions-vans-2016.zip
DATASET_FILE=CO2_vans_v9.csv

while : ;
do
  echo "Trying to connect to HDFS"
  docker exec -i ${NAMENODE} hdfs dfs -ls /

  if [ $? == 0 ]; then
    break;
  else
    sleep 5
  fi
done
echo "Connected."

echo "Give all apps write permission to HDFS (don't use in production !)."
docker exec -i ${NAMENODE} hdfs dfs -chmod 777 / || exit 1

docker exec -i ${NAMENODE} hdfs dfs -ls /${DATASET_FILE}
if [ $? == 0 ]; then
  echo "Dataset already on HDFS"
else
  echo "Downloading example dataset."
  if [ ! -f $DATASET_FILE ]; then
    echo "File ${DATASET_FILE} does not exist, downloading."
    curl $DATASET_URL > $DATASET_FILE_ZIP
    unzip $DATASET_FILE_ZIP
    echo "Cleaning up dataset file"
    sed -i 's/\x0//g' $DATASET_FILE
    rm $DATASET_FILE_ZIP
  else
    echo "File ${DATASET_FILE} exists, will be used."
  fi

  cd $ROOT/..
  echo "Uploading example dataset to HDFS."
  docker-compose run \
    --rm \
    -v$ROOT:/data \
    --entrypoint=bash \
    ${NAMENODE_SERVICE} \
    hdfs dfs -fs "hdfs://${NAMENODE_SERVICE}/" -put /data/${DATASET_FILE} / || exit 1
fi


exit 0
