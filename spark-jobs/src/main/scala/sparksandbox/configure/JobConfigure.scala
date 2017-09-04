package sparksandbox.configure

import java.util.UUID

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Duration, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object JobConfigure {

  def SimpleBatchContext(main: Object): SparkContext = {
    val conf = new SparkConf()
      .setAppName(main.getClass.getSimpleName)
      .setMaster("local")

    val sc = new SparkContext(conf)
    sc.setLogLevel(sc.getConf.getOption("spark.sparksandbox.log_level").getOrElse("WARN"))

    sc
  }

  def BatchContextHdfs(main: Object): SparkContext = {
    val sc = SimpleBatchContext(main)

    sc.hadoopConfiguration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    sc.setCheckpointDir("hdfs://" + JobConfigure.getNamenode(sc) + "/spark_checkpoint/batch")

    sc
  }

  def StreamingContextHDFS(main: Object, batchInterval: Duration): StreamingContext = {
    val sc = BatchContextHdfs(main)
    val ssc = new StreamingContext(sc, batchInterval)

    ssc.checkpoint("hdfs://" + JobConfigure.getNamenode(sc) + "/spark_checkpoint/stream")

    ssc
  }

  def getNamenode(sc: SparkContext): String = {
    sc.getConf.getOption("spark.sparksandbox.hdfs_namenode").getOrElse("localhost:8020")
  }

  def getKafkaBrokers(sc: SparkContext): String = {
    sc.getConf.getOption("spark.sparksandbox.kafka_brokers").getOrElse("localhost:9092")
  }

  def getInputSocket(sc: SparkContext): String = {
    sc.getConf.getOption("spark.sparksandbox.input-socket").getOrElse("localhost")
  }


  def createKafkaStream(ssc: StreamingContext, kafkaTopics: String, brokers: String, randomId: Boolean): DStream[ConsumerRecord[String, String]] = {
    val topicsSet = kafkaTopics.split(",").toSet
    val kafkaParams = Map[String, String](
      "bootstrap.servers" -> brokers,
      "value.deserializer" -> classOf[StringDeserializer].getCanonicalName,
      "key.deserializer" -> classOf[StringDeserializer].getCanonicalName,
      "auto.offset.reset" -> "earliest",
      "group.id" -> ("Message reader" + (if (randomId) UUID.randomUUID().toString else ""))
    )

    KafkaUtils.createDirectStream[String, String](ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](topicsSet, kafkaParams))
  }

}
