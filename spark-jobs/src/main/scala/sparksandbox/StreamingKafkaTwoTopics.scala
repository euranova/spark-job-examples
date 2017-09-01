package sparksandbox

import java.util.{Date, UUID}

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010._
import org.apache.spark.{SparkConf, SparkContext}


object StreamingKafkaTwoTopics {
/*  val KAFKA_BROKERS = "172.19.0.3:9092"
  val HDFS_NAMENODE = "172.19.0.4:8020"
  val SPARK_MASTER = "spark://172.19.0.2:7077"*/

  val KAFKA_BROKERS = "cluster-kafka:9092"
  val HDFS_NAMENODE = "cluster-hdfs-namenode:8020"
  val SPARK_MASTER = "spark://cluster-spark-master:7077"
  //val SPARK_MASTER = "yarn://cluster-hdfs-resourcemanager"
  val checkpointInterval = Seconds(5 * 3) // 40 seconds

  def createKafkaStream(ssc: StreamingContext, kafkaTopics: String, brokers: String, randomId: Boolean): DStream[ConsumerRecord[String, String]] = {
    val topicsSet = kafkaTopics.split(",").toSet
    val kafkaParams = Map[String, String](
      //      "metadata.broker.list" -> brokers,
      "bootstrap.servers" -> brokers,
      "value.deserializer" -> classOf[StringDeserializer].getCanonicalName,
      "key.deserializer" -> classOf[StringDeserializer].getCanonicalName,
      "auto.offset.reset" -> "earliest",
      "group.id" -> ("ploup" + (if (randomId) UUID.randomUUID().toString else ""))
    )

    KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
  }

  // Main inspiration : https://docs.cloud.databricks.com/docs/latest/databricks_guide/07%20Spark%20Streaming/14%20Joining%20DStreams%20With%20Static%20Datasets.html


  def main(args: Array[String]) {
    // Should be some file on your system
    val conf = new SparkConf()

/*      .set("es.nodes.discovery", "false")
      .set("es.nodes.wan.only", "true")
      .set("es.nodes", "127.0.0.1")*/
      .setAppName("Simple Application")
      //      .setMaster("local[2]")
      .setMaster(SPARK_MASTER)
      .setJars(List(
        "target/spark-sandbox-1.0-SNAPSHOT.jar"
        //"target/original-spark-sandbox-1.0-SNAPSHOT.jar"
      ).toArray)
    val sc = new SparkContext(conf)

    sc.setCheckpointDir("hdfs://" + HDFS_NAMENODE + "/checkpointdir")
    val kafkaBrokers = sc.getConf.getOption("spark.sparksandbox.kafka_brokers").getOrElse("localhost:9092")

    sc.setLogLevel("WARN")

    //conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    val ssc = new StreamingContext(sc, Seconds(5))
    ssc.checkpoint("hdfs://" + HDFS_NAMENODE + "/sparkstreamingcheckpoint")

    val stream = createKafkaStream(ssc, "messages", KAFKA_BROKERS, randomId = false)

    val dataFromKafka = stream.map(e => (e.key, (e.value, new Date())))
    dataFromKafka.checkpoint(checkpointInterval)

    experimentMapWithState(sc, ssc, dataFromKafka)

    ssc.remember(Minutes(1))

    ssc.start()
    ssc.awaitTerminationOrTimeout(5 * 5 * 1000)
  }


  case class Message(date: Date, msg: String, name: String, uid: String)

  def experimentMapWithState(sc: SparkContext, ssc: StreamingContext, firstStream: DStream[(String, (String, Date))]): Unit = {
    val secondStream = createKafkaStream(ssc, "trigrams", KAFKA_BROKERS, randomId = true)

    def updateState(batchTime: Time, key: String, value: Option[String], state: State[String]): Option[(String, String)] = {
      val line = value.getOrElse("")
      val output = (key, line)
      state.update(line)
      Some(output)
    }

    val stateSpec = StateSpec.function(updateState _)

    val stateStream = secondStream
      .map(line => (line.key, line.value))
      .checkpoint(checkpointInterval)
      .mapWithState(stateSpec)

    stateStream.print()

    val snapshots = stateStream.stateSnapshots()

    firstStream.leftOuterJoin(snapshots)
      .foreachRDD(rdd => {
        println(">>> New messages")
        rdd.map(line => {
            val uid = line._1
            val name = line._2._2.getOrElse("? "+uid)
            val date = line._2._1._2
            val msg = line._2._1._1
            Message(date, msg, name, uid)
          })
        .collect()
        .foreach(m => {
          println(">>> " + m.name + ": " + m.msg + " \t [" + m.date + "]")
        })
      })
  }

}
