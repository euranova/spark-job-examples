package sparksandbox

import org.apache.spark.SparkConf
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.joda.time.format.DateTimeFormat


object StreamingSimpleStructrured {

  val SPARK_MASTER = "spark://172.20.0.3:7077"

  def main(args: Array[String]) {

    val conf = new SparkConf()
    conf
      //.setMaster(SPARK_MASTER)
      .setMaster("local")
      //.setJars(List("build/libs/spark-cluster-sandbox-1.0-SNAPSHOT.jar").toArray)

    val spark = SparkSession
      .builder
      .config(conf)
      .appName("StructuredNetworkWordCount")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._

    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()


    lazy val f = DateTimeFormat.forPattern("MMM dd HH:mm:ss")

    val splitLines = lines.as[String]
      .map(row => row.splitAt(15))
      //.map(t => (f.parseDateTime(t._1), t._2))


    // Split the lines into words
/*
    val words = lines.as[String].flatMap(_.split(" "))

    // Generate running word count
    val wordCounts = words.groupBy("value").count()
*/



    val query = splitLines.writeStream
      .outputMode("append")
      .foreach(new ForeachWriter[(String, String)] {
        override def open(partitionId: Long, version: Long): Boolean = {
          true
        }

        override def process(value: (String, String)): Unit = {
          println("Row: " + value)
        }

        override def close(errorOrNull: Throwable): Unit = {

        }
      })
      .start()

    // could be: json, csv, etc

    query.awaitTermination()
  }
}
