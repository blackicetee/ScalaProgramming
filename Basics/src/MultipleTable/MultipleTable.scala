package MultipleTable

/**
 * Created by Jason on 13.11.2015.
 */

object MultipleTable {
  private def makeRowSeq(row: Int) = {
    (1 to 10).map(col => (" " * (4 - (col * row).toString.length)) + (col * row).toString)
  }

  private def makeRow(row: Integer) = makeRowSeq(row).mkString(", ")

  def createMultiTable() = {
    (1 to 10).map(row => makeRow(row)).mkString("\n");
  }

  def printGG() = (1 to 10).map(x => println("GG"))

  def main(args: Array[String]) {
    printGG()
  }
}
