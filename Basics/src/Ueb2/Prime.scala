package Ueb2

/**
 * Created by tstegemann on 01.12.2015.
 */

object Prime {
  def main(args: Array[String]) {
    listPrimes(10001).foreach(println)
  }
  def listPrimes(prime: Int): List[Int] = {
    (1 to prime).filter(x => isPrime(x)).toList
  }

  private def isPrime(prime: Int): Boolean = {
    if (prime > 1 && (1 to prime).filter(prime % _ == 0).length == 2 ) true
    else false
  }
}