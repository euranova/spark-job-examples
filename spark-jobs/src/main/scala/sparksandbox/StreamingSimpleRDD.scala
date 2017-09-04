package sparksandbox

import org.apache.spark.streaming.{Minutes, Seconds}
import org.joda.time.DateTime
import sparksandbox.configure.JobConfigure


object StreamingSimpleRDD {

  def main(args: Array[String]) {

    val ssc = JobConfigure.StreamingContextHDFS(this, Seconds(1))

    // requires opening a socket: nc -lk 9999
    val lines = ssc.socketTextStream(JobConfigure.getInputSocket(ssc.sparkContext), 9999)


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
