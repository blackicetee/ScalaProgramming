package test

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import uebung1.Rational

@RunWith(classOf[JUnitRunner])
class RationalTest extends FunSuite {
  
  test("Rational Inititalisation 1") {
    val x = new Rational(1,2)
    assert(x.value === 0.5)
  }
  
  test("Rational Inititalisation 2") {
    val x = new Rational(1,2)
    assertResult("1/2"){x.toString}
  }

  test("Test requirement (denominator!=0)"){
      intercept [IllegalArgumentException] {
        new Rational(1,0)}
  }
}
