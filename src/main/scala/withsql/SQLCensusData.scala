package withsql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SQLCensusData {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[2]").getOrCreate()
    import spark.implicits._
    
    val schema = StructType(Array(
      StructField("age", IntegerType), 
      StructField("workclass", StringType),    
      StructField("fnlwgt", IntegerType),    
      StructField("education", StringType),    
      StructField("educationNum", IntegerType),    
      StructField("maritalStatus", StringType),    
      StructField("occupation", StringType),    
      StructField("relationship", StringType),    
      StructField("race", StringType),    
      StructField("sex", StringType),    
      StructField("capitalGain", IntegerType),    
      StructField("capitalLoss", IntegerType),    
      StructField("hoursPerWeek", IntegerType),    
      StructField("nativeCountry", StringType),    
      StructField("income", StringType)    
    ))
    val data = spark.read.schema(schema).csv("data/adult.csv").cache()

    val n = data.count()
    println("Fraction > 50K = " + data.filter('income === ">50K").count() / n.toDouble)
    println("Average age = "+data.agg(avg('age)).collect().head(0))
    println("Age stats = "+data.describe("age").collect().mkString(", "))
    val over50years = data.filter('age >= 50)
    println("Fraction > 50K in 50+ age group = "+over50years.filter('income === ">50K").count()/over50years.count().toDouble)
    val married = data.filter('maritalStatus === "Married-civ-spouse")
    println("Fraction > 50K in married group = "+married.filter('income === ">50K").count()/married.count().toDouble)
    println("Quartile age = " + data.stat.approxQuantile("age", Array(0.25, 0.5, 0.75), 0.1).mkString(", "))
    println("Fraction by race")
    val raceCounts = data.groupBy('race).
      agg(count('income), count(when('income === ">50K", true)))
    raceCounts.collect().foreach(row => println(row+" "+row.getAs[Long](2)/row.getAs[Long](1).toDouble))
    println("Fraction work more than 40 hrs/week = "+data.filter('hoursPerWeek > 40).count()/n.toDouble)  

    spark.stop()
  }
}