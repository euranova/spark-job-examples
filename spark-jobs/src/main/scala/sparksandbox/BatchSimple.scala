package sparksandbox

import org.apache.spark.{SparkConf, SparkContext}


object BatchSimple {

  def main(args: Array[String]) {
    val conf = new SparkConf()
      .setAppName("BatchSimple")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    println(">>" + sc.getConf.get("spark.driver.host"))
    println(">>" + sc.getConf.get("spark.myapp.input"))
    println(">>" + sc.getConf.get("spark.myapp.output"))

    val numbers = sc.parallelize(Array(0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1))

    val ones = numbers.filter(n => n == 1).count()
    val zeros = numbers.filter(n => n == 0).count()
    val total = numbers.count()

    println("Ones: " + ones)
    println("Zeros: " + zeros)
    println("Total: " + total)
  }
}
