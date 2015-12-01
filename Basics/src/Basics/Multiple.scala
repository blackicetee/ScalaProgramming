package Basics

/**
 * Created by tstegemann on 01.12.2015.
 */
object Multiple {
  def main(args: Array[String]) {
    println(multiple(10))
  }
  def multiple(x: Int): Int = {
    def findMultiples(x: Int): List[Int] = if (x > 0) (1 to x).filter(e => (e % 3 == 0) || (e % 5 == 0)).toList else Nil
    Sum.sumList(findMultiples(x))
  }
}
