package textanalyse

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast

class EntityResolution (sc:SparkContext, dat1:String, dat2:String, stopwordsFile:String, goldStandardFile:String){

  val amazonRDD:RDD[(String, String)]= Utils.getData(dat1, sc)
  val googleRDD:RDD[(String, String)]= Utils.getData(dat2, sc)
  val stopWords:Set[String]= Utils.getStopWords(stopwordsFile)
  val goldStandard:RDD[(String,String)]= Utils.getGoldStandard(goldStandardFile, sc)

  var amazonTokens:RDD[(String, List[String])]= _
  var googleTokens:RDD[(String, List[String])]= _
  var corpusRDD:RDD[(String, List[String])]= _
  var idfDict:Map[String, Double]= _
  
  var idfBroadcast:Broadcast[Map[String,Double]]= _
  var similarities:RDD[(String, String, Double)]= _
  
  def getTokens(data:RDD[(String,String)]):RDD[(String,List[String])]={

    /*
     * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
     * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
     */
    ???
  }
  
  def countTokens(data:RDD[(String,List[String])]):Long={
    
    /*
     * Zählt alle Tokens innerhalb eines RDDs
     * Duplikate sollen dabei nicht eliminiert werden
     */
    
    ???
  }
  
  def findBiggestRecord(data:RDD[(String,List[String])]):(String,List[String])={
    
    /*
     * Findet den Datensatz mit den meisten Tokens
     */
    ???
  }
  def createCorpus={
    
    /*
     * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
     * (amazonRDD und googleRDD) und vereinigt diese und speichert das
     * Ergebnis in corpusRDD
     */
    ???
  }
  
  
  def calculateIDF={
    
    /*
     * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
     * Speichern des Dictionaries in die Variable idfDict
     */
    ???
  }

 
  def simpleSimimilarityCalculation:RDD[(String,String,Double)]={
    
    /*
     * Berechnung der Document-Similarity für alle möglichen 
     * Produktkombinationen aus dem amazonRDD und dem googleRDD
     * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
     * steht, an zweiter die GoogleID und an dritter der Wert
     */
    ???
  }
  
  def findSimilarity(vendorID1:String,vendorID2:String,sim:RDD[(String,String,Double)]):Double={
    
    /*
     * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
     */
    ???
  }
  
 def simpleSimimilarityCalculationWithBroadcast:RDD[(String,String,Double)]={
   
    ???
  }
 
 /*
  * 
  * 	Gold Standard Evaluation
  */
 
  def evaluateModel(goldStandard:RDD[(String, String)]):(Long, Double, Double)={
    
    /*
     * Berechnen Sie die folgenden Kennzahlen:
     * 
     * Anzahl der Duplikate im Sample
     * Durchschnittliche Consinus Similaritaet der Duplikate
     * Durchschnittliche Consinus Similaritaet der Nicht-Duplikate
     * 
     * 
     * Ergebnis-Tripel:
     * (AnzDuplikate, avgCosinus-SimilaritätDuplikate,avgCosinus-SimilaritätNicht-Duplikate)
     */
    
    
    ???
  }
}

object EntityResolution{
  
  def tokenize(s:String, stopws:Set[String]):List[String]= {
    /*
   	* Tokenize splittet einen String in die einzelnen Wörter auf
   	* und entfernt dabei alle Stopwords.
   	* Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
   	*/
     ???
   }

  def getTermFrequencies(tokens:List[String]):Map[String,Double]={
    
    /*
     * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
     * Menge aller Wörter innerhalb eines Dokuments 
     */
    
    ???
  }
   
  def computeSimilarity(record:((String, String),(String, String)), idfDictionary:Map[String,Double], stopWords:Set[String]):(String, String,Double)={
    
    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     */
     ???  
  }
  
  def calculateTF_IDF(terms:List[String], idfDictionary:Map[String,Double]):Map[String,Double]={
    
    /* 
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Mapm die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */
    
    ???
  }
  
  def calculateDotProduct(v1:Map[String,Double], v2:Map[String,Double]):Double={
    
    /*
     * Berechnung des Dot-Products von zwei Vectoren
     */
    ???
  }

  def calculateNorm(vec:Map[String,Double]):Double={
    
    /*
     * Berechnung der Norm eines Vectors
     */
    Math.sqrt(((for (el <- vec.values) yield el*el).sum))
  }
  
  def calculateCosinusSimilarity(doc1:Map[String,Double], doc2:Map[String,Double]):Double={
    
    /* 
     * Berechnung der Cosinus-Similarity für zwei Vectoren
     */
    
    ???
  }
  
  def calculateDocumentSimilarity(doc1:String, doc2:String,idfDictionary:Map[String,Double],stopWords:Set[String]):Double={
   
    /*
     * Berechnung der Document-Similarity für ein Dokument
     */
    ???
  }
  
  def computeSimilarityWithBroadcast(record:((String, String),(String, String)),idfBroadcast:Broadcast[Map[String,Double]], stopWords:Set[String]):(String, String,Double)={
    
    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
     * Sie die erforderlichen Parameter extrahieren
     * Verwenden Sie die Broadcast-Variable.
     */
    ???
  }
}
