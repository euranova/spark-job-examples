package sparksandbox

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{ForeachWriter, SparkSession}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat


object StreamingSimpleRDD {

  val SPARK_MASTER = "spark://172.20.0.3:7077"
  val HDFS_NAMENODE = "172.20.0.2:8020"

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf
      //.setMaster(SPARK_MASTER)
      .setMaster("local[2]")
      .setAppName("UnStructuredStreaming")
      //.setJars(List("build/libs/spark-cluster-sandbox-1.0-SNAPSHOT.jar").toArray)

    val sc = new SparkContext(conf)

    sc.setCheckpointDir("hdfs://" + HDFS_NAMENODE + "/checkpointdir")

    sc.setLogLevel("WARN")

    //conf.set("fs.hdfs.impl", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)

    val ssc = new StreamingContext(sc, Seconds(1))
    ssc.checkpoint("hdfs://" + HDFS_NAMENODE + "/sparkstreamingcheckpoint")

    val lines = ssc.socketTextStream("localhost", 9999)

/*    lines
      .foreachRDD(rdd => {
        println(DateTime.now().toString + " Raw ")
        rdd
      })*/

    lines.map(row => row.splitAt(15)).reduceByKeyAndWindow((a: String, b: String) => {
      b
    }, Seconds(20), Seconds(5))
    .foreachRDD(s => println("Reduce " + s.count))

    lines
      //.window(Seconds(20), Seconds(5))
      .transform(rdd => {
        println(DateTime.now().toString + " RDD " + rdd.count)
        val c = rdd.count
        if(c > 1000){
          println("Sample")
          val s = rdd.sample(true, 1000.0/c,DateTime.now().getMillis)
          println("Sample" + s.count)
          s
        }else{
          rdd
        }
      })
      .foreachRDD(rdd => println("Foreach: " + rdd.count))




    ssc.remember(Minutes(1))

    ssc.start()
    ssc.awaitTerminationOrTimeout(5 * 5 * 1000)
  }
}
