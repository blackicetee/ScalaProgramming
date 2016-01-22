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
  
  /*
   * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
   * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
   */
  def getTokens(data:RDD[(String,String)]):RDD[(String,List[String])]={
    val stopWords_ = this.stopWords
    data.map(X=>(X._1, EntityResolution.tokenize(X._2, stopWords_)))
  }
  
  /*
   * Zählt alle Tokens innerhalb eines RDDs
   * Duplikate sollen dabei nicht eliminiert werden
   */
  def countTokens(data:RDD[(String,List[String])]):Long=
    data.map(X=>X._2.size).reduce((X,Y)=>X+Y)
  
  def findBiggestRecord(data:RDD[(String,List[String])]):(String,List[String])=
    data.toArray().maxBy(_._2.size)
    
  /*
   * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
   * (amazonRDD und googleRDD) und vereinigt diese und speichert das
   * Ergebnis in corpusRDD
   */  
  def createCorpus={
    val amazonRDD_ = this.amazonRDD
    val googleRDD_ = this.googleRDD
    this.corpusRDD=getTokens(amazonRDD_.union(googleRDD_))
  }
  
  /*
   * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
   * Speichern des Dictionaries in die Variable idfDict
   */
  def calculateIDF={
    val corpusRDD_ = corpusRDD.cache
    val nrDocs = corpusRDD_.count()
    val uniqueTokens = corpusRDD_.map(X=>(X._2)).reduce((X,Y)=>(X++Y).distinct)
    val nrDocsOfToken = uniqueTokens.map(X=>(X, corpusRDD_.filter(Y=>Y._2.contains(X)==true).count)).toMap
    this.idfDict = nrDocsOfToken.map(X=>(X._1, nrDocs.toDouble/X._2))
  }

 
  /*
   * Berechnung der Document-Similarity für alle möglichen 
   * Produktkombinationen aus dem amazonRDD und dem googleRDD
   * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
   * steht, an zweiter die GoogleID und an dritter der Wert
   */
  def simpleSimimilarityCalculation:RDD[(String,String,Double)]={
    /*val corpusRDD_ = corpusRDD.cache
    val uniqueTokens = corpusRDD_.map(X=>(X._2)).reduce((X,Y)=>(X++Y).distinct)
    val inverseIndex = uniqueTokens.map(X=>(X, corpusRDD_.filter(Y=>Y._2.contains(X)==true).map(_._1).collect.toList)).toMap
    */
    val amazonRDD_ = this.amazonRDD
    val googleRDD_ = this.googleRDD
    val stopWords_ = this.stopWords
    val idfDict_ = this.idfDict
    val cartesianProd = amazonRDD_.cartesian(googleRDD_)
    cartesianProd.map(X=>EntityResolution.computeSimilarity((X._1,X._2), idfDict_, stopWords_))
  }
  
  /*
   * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
   */
  def findSimilarity(vendorID1:String,vendorID2:String,sim:RDD[(String,String,Double)]):Double=
    sim.filter(X=>(X._1==vendorID1)&&(X._2==vendorID2)).map(X=>X._3).take(1)(0)
  
 
 def simpleSimimilarityCalculationWithBroadcast:RDD[(String,String,Double)]={
   val idfDict_ = sc.broadcast(this.idfDict)
   val cartesianProd = this.amazonRDD.cartesian(this.googleRDD)
   val stopWords_ = this.stopWords
   cartesianProd.map(X=>EntityResolution.computeSimilarityWithBroadcast((X._1,X._2), idfDict_, stopWords_))
  }
 
 
  /*
   *  Gold Standard Evaluation
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
  def evaluateModel(goldStandard:RDD[(String, String)]):(Long, Double, Double)={
    val simpleSim = simpleSimimilarityCalculationWithBroadcast
    val goldL = goldStandard.collect.toList
    val simDup = simpleSim.filter(X=>(goldL.map(X=>X._1).contains(X._1 + " " + X._2)))
    val duplicateCount = simDup.count()
    val avgSimDup = simDup.map(_._3).sum/simDup.count()
    val simNDup = simpleSim.filter(X=>(goldL.map(X=>X._1).contains(X._1 + " " + X._2))==false)
    val avgSimNDup = simNDup.map(_._3).sum/simNDup.count()
    (duplicateCount, avgSimDup, avgSimNDup)
  }
}

object EntityResolution{
  
  /*
    * Tokenize splittet einen String in die einzelnen Wörter auf
    * und entfernt dabei alle Stopwords.
    * Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
    */
  def tokenize(s:String, stopws:Set[String]):List[String]= 
    Utils.tokenizeString(s).filter(X=>stopws.contains(X)==false)

  /*
     * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
     * Menge aller Wörter innerhalb eines Dokuments 
     */
  def getTermFrequencies(tokens:List[String]):Map[String,Double]=
    tokens.toSet.map((X:String)=>(X, tokens.count(_==X)/tokens.size.toDouble)).toMap
  
  /*
   * Bererechnung der Document-Similarity einer Produkt-Kombination
   * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
   * Sie die erforderlichen Parameter extrahieren
   */
  def computeSimilarity(record:((String, String),(String, String)), idfDictionary:Map[String,Double], stopWords:Set[String]):(String, String,Double)=
    (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfDictionary, stopWords))
  
  /* 
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Mapm die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */
  def calculateTF_IDF(terms:List[String], idfDictionary:Map[String,Double]):Map[String,Double]=
    getTermFrequencies(terms).map(X=>(X._1, X._2*idfDictionary.getOrElse(X._1, 0.0)))
  
  /*
   * Berechnung des Dot-Products von zwei Vectoren
   */
  def calculateDotProduct(v1:Map[String,Double], v2:Map[String,Double]):Double=
    v1.map(X=>X._2*v2.getOrElse(X._1, 0.0)).reduce((X,Y)=>X+Y)

  /*
   * Berechnung der Norm eines Vectors
   */
  def calculateNorm(vec:Map[String,Double]):Double=
    Math.sqrt(((for (el <- vec.values) yield el*el).sum))
  
  /* 
   * Berechnung der Cosinus-Similarity für zwei Vectoren
   */
  def calculateCosinusSimilarity(doc1:Map[String,Double], doc2:Map[String,Double]):Double=
    calculateDotProduct(doc1, doc2)/(calculateNorm(doc1)*calculateNorm(doc2))
  
  /*
   * Berechnung der Document-Similarity für ein Dokument
   */
  def calculateDocumentSimilarity(doc1:String, doc2:String,idfDictionary:Map[String,Double],stopWords:Set[String]):Double=
    calculateCosinusSimilarity(calculateTF_IDF(tokenize(doc1, stopWords), idfDictionary), calculateTF_IDF(tokenize(doc2, stopWords), idfDictionary))
  
  /*
   * Bererechnung der Document-Similarity einer Produkt-Kombination
   * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem 
   * Sie die erforderlichen Parameter extrahieren
   * Verwenden Sie die Broadcast-Variable.
   */
  def computeSimilarityWithBroadcast(record:((String, String),(String, String)),idfBroadcast:Broadcast[Map[String,Double]], stopWords:Set[String]):(String, String,Double)={
    val idfDictionary = idfBroadcast.value
    (record._1._1, record._2._1, calculateDocumentSimilarity(record._1._2, record._2._2, idfDictionary, stopWords))
  }
}
