package utility

case class CensusData(
  age: Int,
  workclass: String,
  fnlwgt: Int,
  education: String,
  educationNum: Int,
  maritalStatus: String,
  occupation: String,
  relationship: String,
  race: String,
  sex: String,
  capitalGain: Int,
  capitalLoss: Int,
  hoursPerWeek: Int,
  nativeCountry: String,
  incomeOver50: Boolean)

object CensusData {
  def parseLine(line: String): CensusData = {
    val p = line.split(",").map(_.trim.filter(_ != '"'))
    CensusData(p(0).toInt, p(1), p(2).toInt, p(3), p(4).toInt, p(5),
      p(6), p(7), p(8), p(9), p(10).toInt, p(11).toInt, p(12).toInt, p(13),
      p(14) == ">50K")
  }
}