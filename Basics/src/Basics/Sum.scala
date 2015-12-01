package Basics

/**
 * Created by tstegemann on 01.12.2015.
 */
object Sum {
  def main(args: Array[String]) {
    println(sum(0, 10))
  }
  def sum(x: Int, y: Int): Int = {
    if (x > y) 0
    else x + sum(x + 1, y)
  }

  def sumList(l: List[Int]): Int = {
    if (l.isEmpty) 0
    else l.head + sumList(l.tail)
  }
}
