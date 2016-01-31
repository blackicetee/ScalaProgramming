import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import Ueb3._
/**
 * Created by MrT on 10.12.2015.
 */

@RunWith(classOf[JUnitRunner])
class TestIntList extends FunSuite{
  test("Create IntList") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))
    val intList1 = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))
    assert(intList == intList1)
  }
  test("Create IntList1") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))
    val intList1 = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))
    assertResult("List(4,List(7,List(9,List(11,Empty))))") {
      intList.toString
    }
  }
  test("Create IntList2") {
    val intList = Empty().insert(11).insert(9).insert(7).insert(4)

    assertResult("List(4,List(7,List(9,List(11,Empty))))") {
      intList.toString
    }
  }
  test("IntList contains an element") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))
    assertResult(true) {
      intList.contains(7)
    }
  }
  test("IntList doesn't contain an element") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))
    assertResult(false) {
      intList.contains(8)
    }
  }

  test("nth element of IntList") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))
    assertResult(9) {
      intList.nth(2)
    }
  }
  test("Delete element from IntList") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))
    val assertIntList = new Cons(4, new Cons(9, new Cons(11, Empty())))
    assertResult(assertIntList.toString) {
      intList.delete(7).toString
    }
  }

  test("Delete all element from IntList") {
    val intList = new Cons(4,new Cons(4, new Cons(4,new Cons(4, new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))))))
    val assertIntList = new Cons(7, new Cons(9, new Cons(11, Empty())))
    assertResult(assertIntList.toString) {
      intList.deleteAll(4).toString
    }
  }

  test("set a prefix for IntList") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))
    val prefixList = Cons(1, Cons(2, Cons(3, Empty())))

    assertResult(Cons(1,Cons(2,Cons(3, Cons(4, Cons(7, Cons(9, Cons(11, Empty())))))))) {
      intList.prefix(prefixList)
    }
  }
  test("Traverse IntList") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))

    assertResult(Cons(11,Cons(9,Cons(7, Cons(4, Empty()))))) {
      intList.traverse
    }
  }

  test("InsertS 1 in sorted IntList") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))

    assertResult(Cons(1,Cons(4,Cons(7, Cons(9, new Cons(11, Empty())))))) {
      intList.insertS(1)
    }
  }
  test("InsertS 8 in sorted IntList") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))

    assertResult(Cons(4,Cons(7,Cons(8, Cons(9, new Cons(11, Empty())))))) {
      intList.insertS(8)
    }
  }
  test("InsertSO Cons(1, Cons(8, Empty())) in sorted IntList") {
    val intList = new Cons(4, new Cons(7, new Cons(9, new Cons(11, Empty()))))

    assertResult(Cons(1, Cons(4,Cons(7,Cons(8, Cons(9, new Cons(11, Cons(12, Empty())))))))) {
      intList.insertSO(Cons(1, Cons(8, Cons(12, Empty()))))
    }
  }

  test("InsertionSort Cons(9, Cons(11, Cons(8, Cons(1, Empty())))) IntList") {
    val intList = Cons(9, Cons(11, Cons(8, Cons(1, Empty()))))

    assertResult(Cons(1, Cons(8,Cons(9,Cons(11, Empty()))))) {
      intList.insertionSort
    }
  }

  test("AddAll IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))
    assertResult(31) {
      intList.addAll
    }
  }

  test("MultAll IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))
    assertResult(2772) {
      intList.multAll
    }
  }

  test("Map IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))

    assertResult(Cons(8, Cons(14,Cons(18,Cons(22, Empty()))))) {
      intList.map((x:Int) => x * 2)
    }
  }

  test("Double IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))

    assertResult(Cons(8, Cons(14,Cons(18,Cons(22, Empty()))))) {
      intList.double
    }
  }

  test("Square IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))

    assertResult(Cons(16, Cons(49,Cons(81,Cons(121, Empty()))))) {
      intList.square
    }
  }

  test("Cube IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))

    assertResult(Cons(64, Cons(343,Cons(729,Cons(1331, Empty()))))) {
      intList.cube
    }
  }

  test("Filter IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))
    assertResult(Cons(9, Cons(11, Empty()))) {
      intList.filter(_ > 8)
    }
  }

  test("ReduceSum IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))
    assertResult(31) {
      intList.reduce(0, (r,l) => r + l)
    }
  }

  test("ReduceProduct IntList") {
    val intList = Cons(4, Cons(7, Cons(9, Cons(11, Empty()))))
    assertResult(2772) {
      intList.reduce(1, (r,l) => r * l)
    }
  }

}