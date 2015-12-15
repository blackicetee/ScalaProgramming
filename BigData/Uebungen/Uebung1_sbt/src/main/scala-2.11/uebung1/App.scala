package uebung1

object App {

  def main(args: Array[String]): Unit = {
    
    println("Hier startet das erste Scala Programm!")
    val x= new Rational(2,5)
    val y= new Rational(3,4)
    val z= x.max(y)
    if (z==y) println("Richtiges Ergebnis")
    	else println("Falsches Ergebnis")
   
    // wirft Assertion Failed Exception
    assert(z==y)
  }
}