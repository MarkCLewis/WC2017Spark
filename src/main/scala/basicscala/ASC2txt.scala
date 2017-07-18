package basicscala

case class Layout(start: Int, name: String, len: Int)

object AST2txt extends App {
  val layout = scala.io.Source.fromFile("data/layout.txt").getLines.drop(1).map(line => {
    val p = line.split("\\s+")
    Layout(p(0).toInt, p(1), p(2).toInt)
  }).toArray

  val data = scala.io.Source.fromFile("data/LLCP2015.ASC").getLines
  val pw = new java.io.PrintWriter("data/LLCP2015.csv")
  pw.println(layout.map("\"" + _.name + "\"").mkString(","))
  for (d <- data) {
    val line = for (Layout(s, _, len) <- layout) yield {
      d.substring(s-1, s + len - 1)
    }
    pw.println(line.mkString(","))
  }
  pw.close
}
