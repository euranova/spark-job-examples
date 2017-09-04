
# Examples of spark jobs

## 1) Prepare the platform

```bash
./start_platform.sh
```


## 2) Open UIs

* Spark master: http://localhost:8888/
* HDFS UI: http://[NAMENODE IP]:8888/


## 3) Run jobs

Syntax :

```bash
./run-spark-job.sh [JOB_NAME] [CLUSTER_MASTER]
```

* The value of `JOB_NAME` is one of the objects in
    `./spark-jobs/src/main/scala/sparksandbox/`.
* The value of `CLUSTER_MASTER` can be `local` or the name of a
  spark master, of a mesos master... That value is **optional**
  and is set by default to this cluster's spark master.

Examples :

```bash
# run locally
./run-spark-job.sh BatchSimple local

# run on the cluster
./run-spark-job.sh BatchHdfs

./run-spark-job.sh StreamingSimpleRDD

./run-spark-job.sh StreamingKafkaTwoTopics

```


### StreamingSimpleRDD

First open a socket you can write to :
```
nc -lk 9999
```

### StreamingKafkaTwoTopics

First open two kafka producers to write data to :

#### Trigrams

This topic has been created as a compacted topic: this means that changes
to the names of users will be streamed directly and used immediatly
in new messages.

```bash
./cluster/kafka/producer.sh trigrams
```

The expected input format is `trigram:username`. Example:
```
JBA:Jen Barber
RTR:Roy Trenneman
MMO:Maurice Moss
DRE:Douglas Reynholm
```

#### Messages

```bash
./cluster/kafka/producer.sh messages
```

The expected input format is `trigram:message`. Example:
(taken from the *The IT Crowd*).

```
RTR: We don't neeeeed no educaaation.
MMO: Yes you do; you've just used a double negative
RTR: So, what can we do you for?
JBA: I'm the new head of this department. Is this my office?
RTR: Did she just... I am the head of this department!
MMO: I thought I was.
RTR: It's one of us. It's certainly not her. I'm going to sort this out.
MMO: Roy, you've got a head wound there. Head wound!
RTR: I don't want to be rude or anything but I wasn't informed of any changes to this department.
JBA: Oh did they not tell you about me?
RTR: No, and we are perfectly fine down here thank you very much. We are more than capable of taking care of ourselves.
```
