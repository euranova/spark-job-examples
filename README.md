Examples of spark jobs
=======


* Spark master: http://localhost:8888/
* HDFS UI: http://[NAMENODE IP]:8888/

```bash



./run-spark-job.sh BatchSimple local
./run-spark-job.sh BatchHdfs

./run-spark-job.sh StreamingSimpleRDD
./run-spark-job.sh StreamingSimpleStructured

./run-spark-job.sh StreamingKafkaTwoTopics

```
