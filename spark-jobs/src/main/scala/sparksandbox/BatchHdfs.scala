package sparksandbox

import org.apache.spark.streaming._
import org.apache.spark.{SparkConf, SparkContext}


object BatchHdfs {
  val KAFKA_BROKERS = "cluster-kafka:9092"
  val HDFS_NAMENODE = "cluster-hdfs-namenode:8020"
  val SPARK_MASTER = "spark://cluster-spark-master:7077"
  //val SPARK_MASTER = "yarn://cluster-hdfs-resourcemanager"

  val inputFile = "hdfs://" + HDFS_NAMENODE + "/All_Geographies.data"

  def main(args: Array[String]) {
    // Should be some file on your system
    val conf = new SparkConf()
      .setAppName("Simple Application")
      .setMaster(SPARK_MASTER)
      .setJars(List("target/spark-sandbox-1.0-SNAPSHOT.jar").toArray)

    val sc = new SparkContext(conf)

    sc.setLogLevel("WARN")

    sc.hadoopConfiguration.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)
    sc.hadoopConfiguration.set("fs.file.impl", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)

    println(inputFile)

    val data = sc.textFile(inputFile)

    println("\n\nCount: " + data.count)

    println("\n\ntake 10: ")
    data.take(10).foreach(println)

    val stateStats = data.map(line => line.split(",").toList).filter(list => list.size > 2).map(list => list(2).trim).map(state => (state, 1)) // line2 is the zone

    val stats = stateStats.reduceByKey(_ + _).sortBy(t => t._2)
    stats.cache()

    println("\n\nEntries per zone: ")
    println(stats.count())
    stats.collect().foreach(t => println(t._1 + ": " +t._2))



  }
}
