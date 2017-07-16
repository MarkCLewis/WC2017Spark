package basicscala

import utility.CensusData

/**
 * This program uses basic Scala collection functionality to work with Census data.
 * Date from https://www.kaggle.com/uciml/adult-census-income.
 */
object BasicCensusData {
  def main(args: Array[String]): Unit = {
    val source = scala.io.Source.fromFile("data/adult.csv")
    val data = source.getLines().drop(1).map(CensusData.parseLine).toArray
    source.close()

    println("Fraction > 50K = " + data.count(_.incomeOver50) / data.length.toDouble)
    println("Average age = " + data.map(_.age).sum / data.length.toDouble)
    val over50years = data.filter(_.age >= 50)
    println("Fraction > 50K in 50+ age group = " + over50years.count(_.incomeOver50) / over50years.length.toDouble)
    val married = data.filter(_.maritalStatus == "Married-civ-spouse")
    println("Fraction > 50K in married group = " + married.count(_.incomeOver50) / married.length.toDouble)
    println("Median age = " + data.sortBy(_.age).apply(data.length / 2).age)
    println("Fraction by race")
    for ((race, seq) <- data.groupBy(_.race)) {
      println(s"  $race = ${seq.count(_.incomeOver50) / seq.length.toDouble}")
    }
    println("Fraction work more than 40 hrs/week = " +
      data.foldLeft(0)((tot, cd) => tot + (if (cd.hoursPerWeek > 40) 1 else 0)) / data.length.toDouble)
  }
}