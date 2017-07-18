name := "World Congress Spark Project"
 
version := "1.0"

// At this time Spark doesn't yet support Scala 2.12 
scalaVersion := "2.11.8"
 
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.2.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.2.0"
