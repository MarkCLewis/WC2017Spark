package withsql

import org.apache.spark.sql.SparkSession
import utility.CensusData
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.Encoders

class FracOver50K extends Aggregator[CensusData, (Int, Int), Double] {
  def bufferEncoder: org.apache.spark.sql.Encoder[(Int, Int)] = Encoders.product
  def finish(reduction: (Int, Int)): Double = reduction._1 / reduction._2.toDouble
  def merge(b1: (Int, Int), b2: (Int, Int)): (Int, Int) = (b1._1 + b2._1, b1._2 + b2._2)
  def outputEncoder: org.apache.spark.sql.Encoder[Double] = Encoders.scalaDouble
  def reduce(b: (Int, Int), a: utility.CensusData): (Int, Int) = (b._1 + (if (a.incomeOver50) 1 else 0), b._2 + 1)
  def zero: (Int, Int) = (0, 0)
}

object SQLCensusDataA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
    import spark.implicits._

    val csvFile = spark.read.textFile("data/adult.csv")
    val firstLine = csvFile.first()
    val data = csvFile.filter(_ != firstLine).map(CensusData.parseLine).cache()

    val n = data.count()
    println("Fraction > 50K = " + data.filter(_.incomeOver50).count() / n.toDouble)
    println("Average age = " + data.map(_.age).reduce(_ + _) / n.toDouble)
    val over50years = data.filter(_.age >= 50)
    println("Fraction > 50K in 50+ age group = " + over50years.filter(_.incomeOver50).count() / over50years.count().toDouble)
    val married = data.filter(_.maritalStatus == "Married-civ-spouse")
    println("Fraction > 50K in married group = " + married.filter(_.incomeOver50).count() / married.count().toDouble)
    println("Quartile age = " + data.map(_.age).stat.approxQuantile("value", Array(0.25, 0.5, 0.75), 0.1).mkString(", "))
    println("Fraction by race")
    val fracAgg = (new FracOver50K).toColumn
    val raceCounts = data.groupByKey(_.race).agg(fracAgg)
    raceCounts.collect().foreach(row => println(row))
    println("Fraction work more than 40 hrs/week = " + data.filter(_.hoursPerWeek > 40).count() / n.toDouble)

    spark.stop()
  }
}