package Ueb3

/**
 * Created by MrT on 31.01.2016.
 */
abstract class IntList {
  def head: Int
  def tail: IntList
  def isEmpty: Boolean
  def nth(x: Int): Int
  def contains(x: Int): Boolean
  def prefix(list: IntList): IntList = list match {
    case Empty() => this
    case Cons(head, tail) => new Cons(head, this.prefix(tail))
  }
  def delete(x: Int): IntList
  def deleteAll(x: Int): IntList
  def insert(x: Int): IntList
  def traverse: IntList = {
    def helper(l: IntList, original: IntList): IntList = l match {
      case Empty() => Empty()
      case Cons(head, tail) => if (tail.isEmpty) new Cons(head, helper(original.delete(head), original.delete(head))) else helper(tail, original)
    }
    helper(this, this)
  }
  def insertS(x: Int): IntList
  def insertSO(l: IntList): IntList = {
    def helper(l: IntList, sl: IntList): IntList = l match {
      case Empty() => sl
      case Cons(head, tail) => helper(tail, sl.insertS(head))
    }
    helper(l, this)
  }
  def insertionSort: IntList = {
    def helper(l: IntList, sortedList: IntList): IntList = l match {
      case Empty() => sortedList
      case Cons(head, tail) => helper(tail, sortedList.insertSO(Cons(head, Empty())))
    }
    helper(this, Empty())
  }
  def addAll: Int = {
    def helper(l:IntList): Int = l match {
      case Empty() => 0
      case Cons(head, tail) => head + helper(tail)
    }
    helper(this)
  }
  def multAll: Int = {
    def helper(l: IntList): Int = l match {
      case Empty() => 1
      case Cons(head, tail) => head * helper(tail)
    }
    helper(this)
  }
  /** Variabler Anteil: Funktion, die den aktuellen Wert quadriert, ... (Das ist eine
    * Funktion, die ein Integer auf einen Integer abbildet) */
  def doubleFun(x:Int): Int = 2*x
  def squareFun(x:Int): Int = x*x
  def cubeFun(x:Int): Int = x*x*x
  //Fester Anteil: Durchlauf der Liste
  //intList.map(x => x*x*x) builds cube of every element in intList
  def map(f: Int => Int): IntList = {
    def helper(l: IntList, f: Int => Int): IntList = l match {
      case Empty() => Empty()
      case Cons(head, tail) => helper(tail, f).insert(f.apply(head))
    }
    helper(this, f)
  }
  def double: IntList = map(doubleFun)
  def square: IntList = map(squareFun)
  def cube: IntList = map(cubeFun)

  //double, square und cube mit Anonymen Funktionen
  def anonymDouble: IntList = map(x => 2*x)
  def anonymSquare: IntList = map(x => x*x)
  def anonymCube: IntList = map(x => x*x*x)

  //intList.filter(_ > 8) filtert alle Zahlen die größer als 8 sind aus intList: IntList
  def filter(f: Int => Boolean): IntList = {
    def helper(l: IntList, f: Int => Boolean): IntList = l match {
      case Empty() => Empty()
      case Cons(head, tail) => if (f(head)) helper(tail, f).insert(head) else helper(tail, f)
    }
    helper(this, f)
  }
  //intList.reduce(0, (r,l) => r + l) berechnet die Summe aller elemente der liste
  //intList.reduce(1, (r,l) => r * l) berechnet das Produkt aller elemente der liste
  def reduce(base:  Int, reduceFunction: (Int, Int) => Int): Int = {
    def helper(l: IntList, base: Int, f: (Int, Int) => Int): Int = l match {
      case Empty() => base
      case Cons(head, tail) => f.apply(head, helper(tail, base, f))
    }
    helper(this, base, reduceFunction)
  }
}

case class Cons(val head: Int, val tail: IntList) extends IntList {
  override def nth(x: Int): Int = if (x == 0) head else tail.nth(x - 1)
  override def delete(x: Int): IntList = if (x == head) tail else new Cons(head, tail.delete(x))
  override def deleteAll(x: Int): IntList = if (x == head) tail.deleteAll(x) else new Cons(head, tail.deleteAll(x))
  override def contains(x: Int): Boolean = if (x == head) true else tail.contains(x)
  override def isEmpty: Boolean = false
  override def toString = "List(" + head + "," + tail + ")"
  override def insert(x: Int): IntList = new Cons(x, this)
  override def insertS(x: Int): IntList = if (x <= head) new Cons(x, this) else new Cons(head, tail.insertS(x))
}


case class Empty() extends IntList {
  override def head: Int = throw new Error("head.nil")
  override def nth(x: Int): Int = throw new Error("IndexOutOfBound")
  override def tail: IntList = throw new Error("tail.nil")
  override def delete(x: Int): IntList = Empty()
  override def deleteAll(x: Int): IntList = Empty()
  override def contains(x: Int): Boolean = false
  override def isEmpty: Boolean = true
  override def toString = "Empty"
  override def insert(x: Int): IntList = new Cons(x, Empty())
  override def insertS(x: Int): IntList = new Cons(x, Empty())
}


