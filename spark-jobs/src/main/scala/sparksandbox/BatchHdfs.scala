package sparksandbox

import org.apache.spark.rdd.RDD
import sparksandbox.configure.JobConfigure

import scala.util.Try

object BatchHdfs {

  def main(args: Array[String]) {
    val sc = JobConfigure.BatchContextHdfs(this)
    val hdfsNamenode = JobConfigure.getNamenode(sc)

    val inputFile = "hdfs://" + hdfsNamenode + "/CO2_vans_v9.csv"

    val data = sc.textFile(inputFile)
                  .filter(line => line.trim.length > 0)
                  .cache

    println("\n\nCount All: " + data.count)

    println("\n\ntake 10: ")
    data.take(10).foreach(println)

    val peugeots = data.filter(line => line.contains("PEUGEOT"))


    println("\n\nCount Peugoet: " + peugeots.count)

    def getOneColumnValue(rdd: RDD[String]): RDD[Double] = {
      rdd.map(line => line.split("\t"))
        .map(row => Try(row(21).toDouble))
        .filter(option => option.isSuccess)
        .map(option => option.get)
        .cache
    }

    val colAllData = getOneColumnValue(data)
    val colPeugoets = getOneColumnValue(peugeots)

    println("\n\nAverage Peugoet: " + (colPeugoets.sum / colPeugoets.count))
    println("\n\nAverage Total: " + (colAllData.sum / colAllData.count))

  }
}
