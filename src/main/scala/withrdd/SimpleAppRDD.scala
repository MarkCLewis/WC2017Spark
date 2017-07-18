package withrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

/**
 * Simple example for RDD version of Spark modified from code at:
 *   https://www.nodalpoint.com/development-and-deployment-of-spark-applications-with-scala-eclipse-and-sbt-part-1-installation-configuration/
 */
object SimpleAppRDD {
  def main(args: Array[String]): Unit = {
    val txtFile = "/home/mlewis/Documents/Research/Papers/WorldCongress2017/WC2017Spark/src/main/scala/withrdd/SimpleAppRDD.scala"
    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val txtFileLines = sc.textFile(txtFile, 2).cache()
    val numVals = txtFileLines.filter(line => line.contains("val")).count()
    println("Lines with val: %s".format(numVals))
    sc.stop()
  }
}