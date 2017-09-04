package sparksandbox.configure

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

}
