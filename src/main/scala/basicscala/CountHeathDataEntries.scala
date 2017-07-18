package basicscala

object CountHealthDataEntries extends App {
  val lines = scala.io.Source.fromFile("data/LLCP2015.csv").getLines()
  val headers = lines.next().split(",").map(_.filter(_ != "\""))
  val counts = Array.fill(headers.length)((0,Set[String]()))
  for(line <- lines) {
    val items = line.split(",")
    for(i <- headers.indices) if(items(i).trim.nonEmpty) {
      val (cnt, set) = counts(i)
      counts(i) = (cnt+1, if(set.size > 9) set else set+items(i))
    }
  }
  val tups = headers.zip(counts).sortBy(-_._2._1)
  tups foreach println
}