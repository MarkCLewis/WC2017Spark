package graphs

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph
import org.apache.spark.graphx.lib.ShortestPaths

case class MarvelNode(name: String, ntype: String)

object MarvelGraph {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Graph Application").setMaster("local[*]")
    val sc = new SparkContext(conf)

    // Read in data
    val nodes = scala.io.Source.fromFile("data/nodes.csv").getLines.drop(1).map { line =>
      val comma = line.lastIndexOf(",")
      val (name, ntype) = line.trim.splitAt(comma)
      MarvelNode(name.filter(_ != '"'), ntype.drop(1))
    }.toArray.zipWithIndex.map { case (n, i) => i.toLong -> n }
    
    val indexMap = nodes.map { case (i, n) => n.name.take(20) -> i }.toMap
    
    val edges = scala.io.Source.fromFile("data/hero-network.csv").getLines.drop(1).flatMap { line =>
      val Array(n1, n2) = line.trim.split("\",\"")
      val (name1, name2) = (n1.drop(1).trim, n2.dropRight(1).trim)
      Seq(Edge(indexMap(name1), indexMap(name2), ()), Edge(indexMap(name2), indexMap(name1), ()))
    }.toArray
    
    // Make RDDs and graph
    val nodesRDD = sc.parallelize(nodes)
    val edgesRDD = sc.parallelize(edges)
    val graph = Graph(nodesRDD, edgesRDD)
    
    // Find distance between NIGHTCRAWLER | MUTAN and CAGE, LUKE/CARL LUCA
    val sp = ShortestPaths.run(graph, Seq(indexMap("CAGE, LUKE/CARL LUCA")))
    println(sp.vertices.filter(_._1==indexMap("NIGHTCRAWLER | MUTAN")).first)
    
    // Connected Components
    graph.connectedComponents().vertices.
        filter(t => t._2 != 3 && nodes(t._1.toInt)._2.ntype == "hero").collect.map(t => t._2 -> nodes(t._1.toInt)) foreach println
    
    // Run page rank
    val ranks = graph.pageRank(0.01)
    val ranked = nodesRDD.join(ranks.vertices).sortBy(-_._2._2)
    ranked.take(20) foreach println
  }
}