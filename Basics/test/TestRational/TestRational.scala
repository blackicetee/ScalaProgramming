package TestRational

import Ueb1.Rational
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by Mr.T on 29.10.2015.
 */
@RunWith(classOf[JUnitRunner])
class TestRational extends FunSuite {
  test("Rational initialisation:") {
    val x = new Rational(1, 2)
    assert(x.toString === "1/2")
  }
  test("Test Add: 1/2 + 1/6 = 2/3") {
    val x = new Rational(1, 2)
    val y = x.add(new Rational(1, 6))
    assertResult("2/3") {
      y.toString
    }
  }
  test("Test requirement (denominator!=0)") {
    intercept [IllegalArgumentException] {
        new Rational(1,0)
      }
  }
  test("Test Sub/- : 1/2 - 1/6 = 1/3") {
    val x = new Rational(1, 2)
    val y = x - (new Rational(1, 6))
    assertResult("1/3") {
      y.toString
    }
  }

  test("Test Neg: neg(1/3) = -1/3") {
    val x = new Rational(1, 3)
    val y = x.neg
    assertResult("-1/3") {
      y.toString
    }
  }
}
