package relationale_algebra

import mapreduce._

object Basics {

  type Table=List[(String,List[Any])]

  def projection(data:Table, cols:List[Int]):Table= {
    
    val config= Map("cols"->cols)
    val d= data map ((-1,_))
    val l=
	BasicOperations.mapReduce[Int,(String, List[Any]),List[Any], List[Any], Int,(String, List[Any])](	    
		(X,config)=> {
			config.get("cols") match{
 		  		case Some(l:List[Int]) => { val col= l map (Y=> X._2._2(Y));List((col,col))} 
		  		case _ => throw new Error("No columns for Projection")
			}
		 },
		 
		(Y,config)=>  List((-1, ("projection",Y._1))),
		d,
		config
	) 
	l map (X=>X._2)
  }
  
  def projection(data:Table, pFun:List[Any]=>List[Any]):Table= {
    
    val config= Map("pFun"->pFun)
    val d= data map ((-1,_))
    val l=
	BasicOperations.mapReduce[Int,(String, List[Any]),List[Any], List[Any], Int,(String, List[Any])](	    
		(X,config)=> {
			config.get("pFun") match{
 		  		case Some(f:(List[Any]=>List[Any])) => { val col= f(X._2._2);List((col,col))} 
		  		case _ => throw new Error("No Projection")
			}
		 },
		 
		(Y,config)=>  List((-1, ("projection",Y._1))),
		d,
		config
	) 
	l map (X=>X._2)
  }
  
  def projectionBag(data:Table, cols:List[Int]):Table= ???

  def eliminateDuplicates(data:Table):Table= ???
  
  
  def rename(data:Table, newName:String):Table={
    
    val config= Map("newName"->newName)
    val d= data map ((-1,_))

 	BasicOperations.mapReduce[Int,(String, List[Any]),List[Any], List[Any], Int,(String, List[Any])](
    
 		(X, config) => List((X._2._2,X._2._2)),
	    (Y, config) => 
	      
	      config.get("newName") match {
 		    case Some(newName:String) => {Y._2 map (Z=>(-1,(newName,Z)))}
 		    case _ => throw new Error("No Selection Function!")
 		  },

	    d,
	    config
	) map (X=>X._2)

  }
  
  def selection(data:Table, pred:List[Any]=>Boolean):Table= ???
  
    def naturalJoin(data1:Table, tname1:String, joinAttribs1:List[Int], data2:Table, tname2:String, joinAttribs2:List[Int], projection:List[Any]=>List[Any]):Table={     
    
    // data1 und data2: Tabellen, die gejoint werden soll
    // joinAttribs1 und joinAttribs2: Liste der join-Attribute. Ist eine der Listen leer, wird ein kartesisches Produkt ausgef�hrt
    // tname und tname2: Tabellennamen
    // projection: Attribute, die in der Ergebnismenge sind
    
    val config= if (joinAttribs1.isEmpty || joinAttribs2.isEmpty) Map("tn1"->tname1, "tn2"->tname2, "project"-> projection)
      else Map("att1"->joinAttribs1, "tn1"->tname1, "att2"->joinAttribs2, "tn2"->tname2,"project"-> projection )
      
    val d= (data1 ++ data2) map ((-1,_))
 	BasicOperations.mapReduce[Int,(String, List[Any]),List[Any], (String, List[Any]), Int,(String, List[Any])](
    
 		(X, config) => {
 		  (config.get("att1"), config.get("tn1"), config.get("att2"), config.get("tn2")) match {
 		    case (Some(att1:List[Int]), Some(tn1:String), Some(att2:List[Int]), Some(tn2:String)) => {
 		      
 		      if (X._2._1.equals(tn1)) { 
 		    	  val l= att1 map (X._2._2(_))
 		    	  List((l, X._2))
 		      }
 		      	else { 
 		    	  val l= att2 map (X._2._2(_))
 		    	  List((l, X._2))
 		      	} 
 		    }
 		    
 		    case (_, Some(tn1:String), _, Some(tn2:String)) => List((List(),X._2))
 		    
 		    case _ => throw new Error("No attrib-List or tablenames for natural join!")
 		  }
 		},
	    (Y, config) => {
	      (config.get("tn1"), config.get("tn2"), config.get("project")) match {
	        
	        case (Some(tn1:String), Some(tn2:String), Some(proj:(List[Any]=>List[Any]))) => {
	          
	        	val l= for (t1 <- Y._2; t2 <- Y._2 if (t1._1.equals(tn1) && t2._1.equals(tn2))) yield t1._2++t2._2
	        	
	        		l map (Z=>(-1,("njoin",proj(Z))))
	        	 }
	        case _ => throw new Error("No tablenames!")
	      }},
	    d,
	    config
	) map (X=>X._2)
  }

  def union(data1:Table, data2:Table):Table= ???
  
  def unionAll(data1:Table, data2:Table):Table= ???
  
  def intersection(data1:Table, data2:Table):Table= ???
  
  def difference(data1:Table, tname:String, data2:Table):Table= ???  
  
   
  def groupBy(data:Table, gbattrib:List[Int], aggFun:String, attr:Int):Table= {
    
    val d= data map ((-1,_))
    val config= Map("attrL"->gbattrib, "aggFun"->aggFun, "attr"->attr)
    BasicOperations.mapReduce[Int,(String, List[Any]),List[Any], (String,List[Any]), Int,(String, List[Any])](
	    
    	(X, config) => { config.get("attrL") match {
    			  
    			case Some(attrL:List[Int]) => {
    				  List((attrL map (Z=> X._2._2(Z)), X._2))
    			  	}
    			case _ => throw new Error("No GroupBy attribbutes!")
    		}
    	},  
    	(Y, config) => {
    		 (config.get("aggFun"),config.get("attr")) match {
    		   
    		   	case (Some(aggFun:String), Some(attr:Int)) =>{
    		     
    		   		val agg= aggFun match{
    		        
    		   			case "count" => Y._2.length
    		   			case "sum" => Y._2.foldLeft(0)((A,B)=> A+B._2(attr).asInstanceOf[Int]) 
    		   			case "avg" => Y._2.foldLeft(0)((A,B)=> A+B._2(attr).asInstanceOf[Int])/Y._2.length.toDouble
    		   			case "max" => Y._2.foldLeft(Y._2(0)._2(attr).asInstanceOf[Int])((A,B)=> B._2(attr).asInstanceOf[Int].max(A)) 
    		   			case "min" => Y._2.foldLeft(Y._2(0)._2(attr).asInstanceOf[Int])((A,B)=> B._2(attr).asInstanceOf[Int].min(A))
    		   			case _ => throw new Error("No aggregate function!")
    		   		}
    		   		List((-1, ("groupBy", Y._1 ++ List (agg))))
    		   } 
    		   case _ => throw new Error("No aggregation function or aggregation attribute!")
    		 }  	
    	},
    	d,
    	config
	    ) map (X=>X._2)
  	}
  
    def groupByWithHaving(data:Table, gbattrib:List[Int], aggFunList:List[(String, Int)], having:List[Any]=>Boolean, projection:List[Int]):Table= ???

    // gbattrib: Liste der Gruppierungsattribute
    // Wenn leere Liste �bergeben wird, dann wird nicht gruppiert und die Aggregationsfunktion auf
    // die gesamte Tabelle angewendet.
    
    // aggFun: Liste von Tupeln bestehend aus der Aggregationsfunktion und der Stelle des Attributs auf
    // das die Funktion angewendet werden soll
    
    // having: Having-Klausel - wird auf die Aggregationsattribute angewendet
      
    // projection: Attribute, die am Ende der Operation in der Ergebnismenge sind
    

  
  def printTable(data:Table):Unit={
    
    data foreach {Y=> Y._2.foreach (X=>print(" "+ X ));println("")}
    	
  }
}