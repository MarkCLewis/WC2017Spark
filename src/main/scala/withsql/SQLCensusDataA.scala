package withsql

import org.apache.spark.sql.SparkSession
import utility.CensusData
import org.apache.spark.sql.functions._

object SQLCensusDataA {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[2]").getOrCreate()
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
    println("Median age = " + data.sort("age"))//.zipWithIndex().filter(_._2 == n / 2).collect()(0)._1.age)
    println("Fraction by race")
//    val raceCounts = data.map(cd => cd.race -> cd).aggregateByKey((0, 0))(
//      { case ((tot, over), cd) => (tot + 1, over + (if (cd.incomeOver50) 1 else 0)) },
//      { case ((tot1, over1), (tot2, over2)) => (tot1 + tot2, over1 + over2) }).collect()
//    for ((race, (tot, over)) <- raceCounts) {
//      println(s"  $race = ${over / tot.toDouble}")
//    }
    println("Fraction work more than 40 hrs/week = " + data.filter(_.hoursPerWeek > 40).count() / n.toDouble)

    spark.stop()
  }
}