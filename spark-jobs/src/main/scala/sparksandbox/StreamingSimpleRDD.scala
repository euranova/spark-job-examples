package sparksandbox

import org.apache.spark.streaming.{Minutes, Seconds}
import sparksandbox.configure.JobConfigure


object StreamingSimpleRDD {

  def main(args: Array[String]) {

    val ssc = JobConfigure.StreamingContextHDFS(this, Seconds(1))

    val lines = JobConfigure.createKafkaStream(ssc, "words",
                  JobConfigure.getKafkaBrokers(ssc.sparkContext), randomId = false)
                  .map(e => e.value)

    lines.map(row => row.split(" ").toList)
         .reduceByWindow((a: List[String], b: List[String]) => {
            a ++ b
         }, Seconds(20), Seconds(5))
         .map(a => a.size)
         .foreachRDD(rdd => {
           rdd.collect().foreach(count => println("Words: " + count))
         })
    
   ssc.remember(Minutes(1))

    ssc.start()
    ssc.awaitTerminationOrTimeout(5 * 5 * 1000)
  }
}
