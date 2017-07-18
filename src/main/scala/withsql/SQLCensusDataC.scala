package withsql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SQLCensusDataC {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("Simple Application").master("local[*]").getOrCreate()
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
    val data = spark.read.schema(schema).option("header", true).csv("data/adult.csv").cache()
    data.createOrReplaceTempView("adult")

    val n = spark.sql("select count(age) from adult").head().getAs[Long](0)
    println("Fraction > 50K = " + spark.sql(
        "select c1.cnt/c2.cnt from (select count(income) as cnt from adult) as c2 cross join (select count(income) as cnt from adult where income = '>50K') as c1").head.getAs[Double](0))

    spark.stop()
  }
}