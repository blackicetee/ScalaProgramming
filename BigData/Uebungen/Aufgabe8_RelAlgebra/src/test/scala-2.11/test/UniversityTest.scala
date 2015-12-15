package test

import org.scalatest.FunSuite
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import relationale_algebra._

@RunWith(classOf[JUnitRunner])
class UniversityTest extends FunSuite{

  val data_profs= List(("P",List(2125,"Sokrates","C4",226)), ("P",List(2126, "Russel", "C4", 232)),   
      ("P", List(2127, "Kopernikus", "C3", 310)), ("P",List(2133, "Popper", "C3", 52)), 
      ("P", List(2134, "Augustinus", "C3", 309)), ("P", List(2136, "Curie", "C4", 36)), 
      ("P",List(2137, "Kant", "C4", 7))) 
       
  val data_studenten=List(("S",List(24002, "Xenokrates", 18)), ("S", List(25403, "Jonas", 12)), 
     ("S", List(26120, "Fichte", 10)), ("S",List(26830, "Aristoxenos", 8)), 
     ("S", List(27550, "Schopenhauer", 6)), ("S",List(28106, "Carnap", 3)), 
     ("S", List(29120, "Theophrastos", 2)), ("S",List(29555, "Feuerbach", 2)))
 
  val data_vorlesungen= List(("V", List(5001, "Grundzuege", 4, 2137)),("V",List(5041, "Ethik", 4, 2125)),
	("V",List(5043, "Erkenntnistheorie", 3, 2126)), ("V",List(5049, "Maeeutik", 2, 2125)),
	("V",List(4052, "Logik", 4, 2125)),("V",List(5052, "Wissenschaftstheorie", 3, 2126)),
	("V",List(5216, "Bioethik", 2, 2126)),("V",List(5259, "Der Wiener Kreis", 2, 2133)), 
	("V",List(5022, "Glaube und Wissen", 2, 2134)),("V",List(4630, "Die 3 Kritiken", 4, 2137)))
 
  val data_hoeren= List(("H",List(26120, 5001)),("H",List(27550, 5001)),("H",List(27550, 4052)),("H",List(28106, 5041)), 
	("H",List(28106, 5052)),("H",List(28106, 5216)),("H",List(28106, 5259)),("H",List(29120, 5001)),("H",List(29120, 5041)), 
	("H",List(29120, 5049)),("H",List(29555, 5022)), ("H",List(25403, 5022)), ("H",List(29555, 5001)))  

  val data_voraussetzen= List(("VO",List(5001, 5041)),("VO",List(5001, 5043)),("VO",List(5001, 5049)), ("VO",List(5041, 5216)),  
	("VO",List(5043, 5052)),("VO",List(5041, 5052)),("VO",List(5052, 5259)))

  val data_assis= List(("A",List(3002, "Platon", "Ideenlehre", 2125)),("A",List(3003, "Aristoteles", "Syllogistik", 2125)),
    ("A",List(3004, "Wittgenstein", "Sprachtheorie", 2126)),("A",List(3005, "Rhetikus", "Planetenbewegung", 2127)),
    ("A",List(3006, "Newton", "Keplersche Gesetze", 2127)),("A", List(3007, "Spinoza", "Gott und Natur", 2134)))

  val data_pruefen= List(("PR",List[Any](28106, 5001, 2126, 1.0)),("PR",List[Any](25403, 5041, 2125, 2.0)),
	("PR", List[Any](27550, 4630, 2137, 2.0)))


	val data= data_profs ++ data_studenten ++ data_vorlesungen ++ data_hoeren ++ data_voraussetzen ++ data_assis ++ data_pruefen
	
	test("print"){
    
	  Basics.printTable(data_profs)
    
  }
  
  	test("projection 1"){
    
  	  	  println("------------------------")
	  Basics.printTable(Basics.projection(data_profs, List(0,1)))
    
  }
  	
  test("projection 2"){

    println("------------------------")
	  Basics.printTable(Basics.projectionBag(data_profs, List(2)))
    
  }
  
  test("projection 3"){
    
	  println("------------------------")
	  Basics.printTable(Basics.eliminateDuplicates(Basics.projectionBag(data_profs, List(2))))
    
  }
 
  test("projection with Doubles"){

      println("------------------------")
 	  val p= Basics.projection(data_pruefen, List(0,1))
  	  Basics.printTable(p)
  }
  test("selection 1"){
    
	  println("------------------------")
	  Basics.printTable(Basics.selection(data_studenten, X => X(2).asInstanceOf[Int]>4))
    
  }
  
  test("natural Join 1"){
    
	  println("------------------------")
	  Basics.printTable(Basics.naturalJoin(data_vorlesungen, "V", List(3), data_profs, "P", List(0), X=>X))
    
  }
  
  test("union without duplicates"){
    
 	  println("------------------------")
 	  val d1= Basics.projection(("S",List(111,"Sokrates",3))::data_studenten, List(1))
  	  val d2= Basics.projection(data_profs, List(1))
  	  Basics.printTable(Basics.union(d1,d2))
  }
  
  test("union with duplicates"){
 	  println("------------------------")
 	  println("Wie hei�en alle Studenten und Professoren?")
 	  val d1= Basics.projection(("S",List(111,"Sokrates",3))::data_studenten, List(1))
  	  val d2= Basics.projection(data_profs, List(1))
  	  Basics.printTable(Basics.unionAll(d1,d2))
  }
  
  test("intersection"){
 	  println("------------------------")
 	  println("Welche Studenten haben sich in einer VL pruefen lassen, die sie auch geh�rt haben?")
 	  val p= Basics.projection(data_pruefen, List(0,1))
  	  val i= Basics.intersection(p, ("H",List(28106, 5001))::data_hoeren)
  	  val j= Basics.naturalJoin(i, "intersection", List(0), data_studenten, "S", List(0),X=>List(X(2),X(3)))
  	  Basics.printTable(j)
  }
  
  test("difference"){
 	  println("------------------------")
 	  println("Welche Studenten haben keine VL gehoert?")
 	  val s= Basics.projection(data_studenten, List(0))
   	  val h= Basics.rename(Basics.projection(data_hoeren, List(0)), "hoeren")
  	  val i= Basics.difference(s,"projection",h)
  	  val j= Basics.naturalJoin(i, "difference", List(0), data_studenten, "S", List(0),X=>List(X(1),X(2)))
  	  Basics.printTable(j)
  }
 
  test("group by 1"){
 	  println("------------------------")
 	  println("Wie viele Vorlesungen haben die einzelnen Studenten geh�rt?")
 	  val j= Basics.naturalJoin(data_studenten, "S",List(0), data_hoeren, "H", List(0),X=>X)
  	  val gb= Basics.groupByWithHaving(j,List(0,1),List(("count",-1)),X=> true, List())
  	  Basics.printTable(gb)
  }

  test("group by 2"){
 	  println("------------------------")
 	  println("Was ist die durchschnittliche Dauer der einzelnen Vorlesungen der Professoren?")
 	  val j= Basics.naturalJoin(data_profs, "P",List(0), data_vorlesungen, "V", List(3),X=>X)
  	  val gb= Basics.groupByWithHaving(j,List(0,1),List(("avg",6)), X=> true, List())
  	  Basics.printTable(gb)
  }
  
  test("group by 3"){
 	  println("------------------------")
 	  println("Wie viele Vorlesungen haben die einzelnen Studenten geh�rt und wie viele Semesterwochenstunden sind das?")
 	  val j= Basics.naturalJoin(Basics.naturalJoin(data_studenten, "S",List(0), data_hoeren, "H", List(0),X=>X),"njoin", List(4), data_vorlesungen, "V", List(0),X=>X)
  	  val gb= Basics.groupByWithHaving(j,List(0,1),List(("count",-1),("sum",7), ("max",7),("min",7)), X=> true, List())
  	  Basics.printTable(gb)
  }
  
  test("average Function without group By"){
    
      println("------------------------")
      println("Was ist die durchschnittliche Semesterzahl der Studenten?")
      val j= Basics.groupByWithHaving(data_studenten, List(), List(("avg",2)), X=> true, List())
      Basics.printTable(j)
  }
  
  test("Having-Klausel"){
 	  println("------------------------")
 	  println("Welche C4-Professoren halten durchschnittlich lange Vorlesungen (avg(SWS)>3)?")
 	  val j= Basics.naturalJoin(data_profs, "P",List(0), data_vorlesungen, "V", List(3),X=>X)
 	  val s= Basics.selection(j, X=> X(2).equals("C4"))
  	  val gb= Basics.groupByWithHaving(s,List(0,1),List(("sum",6), ("avg",6)), X=>X(1).asInstanceOf[Double]>3, List(1,2))
  	  Basics.printTable(gb)
  }
  
  test("Kartesisches Produkt"){
 	  println("------------------------")
 	  println("Welche Vorlesungen halten die C4-Professoren?")
 	  val j= Basics.naturalJoin(data_profs, "P",List(), data_vorlesungen, "V", List(),X=>X)
 	  val s= Basics.selection(j, X=> X(2).equals("C4") && X(0)==X(7))
 	  Basics.printTable(s)
  }
}