package sparksandbox

import sparksandbox.configure.JobConfigure

object BatchSimple {

  def main(args: Array[String]) {
    val sc = JobConfigure.SimpleBatchContext(this)

    val numbers = sc.parallelize(Array(0, 1, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1, 0, 1), 4)

    val ones = numbers.filter(n => n == 1).count()
    val zeros = numbers.filter(n => n == 0).count()
    val total = numbers.count()

    println("Ones: " + ones)
    println("Zeros: " + zeros)
    println("Total: " + total)
  }
}

