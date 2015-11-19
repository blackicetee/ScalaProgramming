package MultipleTable

/**
 * Created by Jason on 13.11.2015.
 */
class MultipleTable {

  def makeRowSeq(row: Integer) = {
    for (col <- 1 to 10) yield {
      val prod = (row * col).toString
      val padding = " " * (4 - prod.length)
      padding + prod
    }
  }

  def makeRow(row: Integer) = makeRowSeq(row).mkString(", ")

  def createMultiTable() = {
    val rowSeq =
      for (row <- 1 to 10) yield {
        makeRow(row)
    }
    rowSeq.toString()
  }
}

object MultipleTable {
  def main (args: Array[String]){
    val mT = new MultipleTable
    print(mT.createMultiTable())
  }
}
