package Ueb1

/**
 * Created by Mr.T on 29.10.2015.
 */
class Rational(numerator: Int, denominator: Int) {
  require(denominator != 0, "Denominator must be != 0") //Throws IllegalArgumentException
  println("A rational was created ...") //Is part of the constructor
  def this (denom: Int) = this(1, denom)
  def num: Int = numerator //Getter for numerator
  def denom: Int = denominator //Getter for denominator
  def value: Double = (num.toDouble / denom) //Converting
  def add(y: Rational): Rational = new Rational(numerator * y.denom + y.num * denom, denominator * y.denom)
  override def toString: String = num + "/" + denom
}

object Rational extends App {
  println("Test rational class: ")
  val rationalObj1 = new Rational(1,2)
  println(rationalObj1.denom, rationalObj1.num, rationalObj1.value, rationalObj1.toString)
  val x = new Rational(1, 2)
  val y = x.add(new Rational(1, 6))
  println(y.toString)
}
