package Ueb1

/**
 * Created by Mr.T on 29.10.2015.
 */
class Rational(numerator: Int, denominator: Int) {
  require(denominator != 0, "Denominator must be != 0") //Throws IllegalArgumentException

  //Is part of the constructor
  println("A rational: " + numerator + "/" + denominator + " was created ...")
  def this(denom: Int) = this(1, denom)

  private def gcd(a: Int, b: Int): Int = if(b == 0)a else gcd(b, a % b)

  //Getter for numerator
  def num: Int = numerator / gcd(numerator, denominator)

  //Getter for denominator
  def denom: Int = denominator / gcd(numerator, denominator)

  //Converting
  def value: Double = (num.toDouble / denom)


  def add(y: Rational): Rational = new Rational(numerator * y.denom + y.num * denom, denominator * y.denom)
  def +(other: Rational): Rational = new Rational(numerator * other.denom + other.num * denom, denominator * other.denom)

  def sub(other: Rational): Rational = new Rational(numerator * other.denom - other.num * denom, denominator * other.denom)
  def -(other: Rational): Rational = new Rational(numerator * other.denom - other.num * denom, denominator * other.denom)

  def neg: Rational = new Rational(-num, denom)

  override def toString: String = num + "/" + denom
}

object Rational {
  def main(args: Array[String]): Unit = {
    println("Test rational class: ")
    val rationalObj1 = new Rational(1, 2)
    println(rationalObj1.denom, rationalObj1.num, rationalObj1.value, rationalObj1.toString)
    val x = new Rational(1, 2)
    val y = x.add(new Rational(1, 6))
    println(y.toString)
    println(x.neg)
  }

}
