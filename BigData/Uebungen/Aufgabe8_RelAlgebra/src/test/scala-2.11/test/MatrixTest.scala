package test

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import relationale_algebra._

@RunWith(classOf[JUnitRunner])
class MatrixTest extends FunSuite{

  val data_tabA= List(("A",List(0,0,1)),("A",List(0,1,2)),("A",List(0,2,3)),("A",List(1,0,4)),("A",List(1,1,5)),("A",List(1,2,6)),("A",List(2,0,7)),("A",List(2,1,8)),("A",List(2,2,9)))
  val data_tabB= List(("B",List(0,0,11)),("B",List(0,1,12)),("B",List(1,0,13)),("B",List(1,1,14)),("B",List(2,0,15)),("B",List(2,1,16)))    
  
  /* 1 2 3				11 12				082 088
   * 4 5 6		*		13 14		= 		199 214
   * 7 8 9				15 16				316 340
   */
  
  
      /*
  select tmp.row, tmp.col, sum (tmp.val) from
     (select a.row as row, b.col as col,a.val*b.val as val from a,b where a.col=b.row) tmp group by tmp.row, tmp.col;    
      */
   
  test("Matrix Multiplikation 1"){
    
    println("---------------------")
    val join= Basics.naturalJoin(data_tabA, "A", List(1), data_tabB, "B", List(0), X=> List(X(0),X(4), X(2).asInstanceOf[Int]*X(5).asInstanceOf[Int]))
    val gb= Basics.groupByWithHaving(join, List(0,1), List(("sum",2)), Y=> true, List())
    Basics.printTable(gb)
  }
  
   val data_tabAS= List(("A",List(0,0,1)),("A",List(0,1,2)),("A",List(0,2,3)),("A",List(1,0,4)),("A",List(1,1,5)),("A",List(1,2,6)),("A",List(2,0,7)),("A",List(2,1,8)))
  
   
  /* 1 2 3				11 12				082 088
   * 4 5 6		*		13 14		= 		199 214
   * 7 8 0				15 16				181 196
   */
  
  test("Matrix Multiplikation 2"){

    println("---------------------")
    val join= Basics.naturalJoin(data_tabAS, "A", List(1), data_tabB, "B", List(0), X=> List(X(0),X(4), X(2).asInstanceOf[Int]*X(5).asInstanceOf[Int]))   
    val gb= Basics.groupByWithHaving(join, List(0,1), List(("sum",2)), Y=> true, List())
    Basics.printTable(gb)
  }

}