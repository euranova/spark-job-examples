package sparksandbox.configure

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

    sc
  }

  def getNamenode(sc: SparkContext): String = {
    sc.getConf.getOption("spark.sparksandbox.hdfs_namenode").getOrElse("localhost:8020")
  }

}
