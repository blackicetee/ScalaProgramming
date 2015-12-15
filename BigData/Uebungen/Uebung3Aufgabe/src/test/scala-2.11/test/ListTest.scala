package test

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import list._

@RunWith(classOf[JUnitRunner])
class ListTest extends FunSuite {

  
	trait TestLists{ 

	  val l1= Empty.insert(4).insert(5).insert(2).insert(3).insert(9)
	  val l2= Empty.insert(4).insert(5).insert(2).insert(3).insert(9).insert(11)
	  val l3= Empty.insert(4).insert(5).insert(3).insert(9).insert(11)
 	  val l4= Empty.insert(11).insert(9).insert(5).insert(4).insert(3).insert(2)
 	  val l5= Empty.insert(11).insert(9).insert(8).insert(5).insert(4).insert(3).insert(2)
 	  val l6= Empty.insert(2).insert(9).insert(8).insert(4).insert(5).insert(3).insert(11)
	}
	
	test("toString Method") {
	  
		new TestLists{
    	
			assert("Cons(9,Cons(3,Cons(2,Cons(5,Cons(4,Empty)))))"===l1.toString)
		}
	}

	test("insert Method"){
	  
	  new TestLists{
	    
	    assert(l2===l1.insert(11))
	  }
	}
	
	test("insertS Method"){
	  
	  new TestLists{
	    
	    assert(l5===l4.insertS(8))
	  }
	}
	
	test("insertSO Method"){
	  
	  new TestLists{
	    
	    assert(l5===l4.insertSO(8))
	  }
	}
	
	test("delete Method"){
	  
	  new TestLists{
	    
	    assert(l3===l2.delete(2))
	  }
	}
	
	test("insertsertionSort Method"){
	  
	  new TestLists{
	    
	    assert(l5===l6.insertionSort)
	  }
	}
	
}