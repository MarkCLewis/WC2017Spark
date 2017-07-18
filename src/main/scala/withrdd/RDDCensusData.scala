package withrdd

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import utility.CensusData
import org.apache.spark.rdd.RDD._

object RDDCensusData {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Sample Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val csvFileLines = sc.textFile("data/adult.csv", 2).mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
    val data = csvFileLines.map(CensusData.parseLine).cache()

    val n = data.count()
    println("Fraction > 50K = " + data.filter(_.incomeOver50).count() / n.toDouble)
    println("Average age = " + data.map(_.age).sum / n.toDouble)
    val over50years = data.filter(_.age >= 50)
    println("Fraction > 50K in 50+ age group = " + over50years.filter(_.incomeOver50).count() / over50years.count().toDouble)
    val married = data.filter(_.maritalStatus == "Married-civ-spouse")
    println("Fraction > 50K in married group = " + married.filter(_.incomeOver50).count() / married.count().toDouble)
    println("Median age = " + data.sortBy(_.age).zipWithIndex().filter(_._2 == n / 2).collect()(0)._1.age)
    println("Fraction by race")
    val raceCounts = data.map(cd => cd.race -> cd).aggregateByKey((0, 0))(
      { case ((tot, over), cd) => (tot + 1, over + (if (cd.incomeOver50) 1 else 0)) },
      { case ((tot1, over1), (tot2, over2)) => (tot1 + tot2, over1 + over2) }).collect()
    for ((race, (tot, over)) <- raceCounts) {
      println(s"  $race = ${over / tot.toDouble}")
    }
    println("Fraction work more than 40 hrs/week = " + data.filter(_.hoursPerWeek > 40).count() / n.toDouble)

    sc.stop()
  }
}