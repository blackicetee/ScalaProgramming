package TestRational

import Ueb1.Rational
import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

/**
 * Created by Mr.T on 29.10.2015.
 */
@RunWith(classOf[JUnitRunner])
class TestRational {

  class TestRational extends FunSuite {
    test("Rational Inititalisation:") {
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
  }
}
