package textanalyse

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

class EntityResolution(sc: SparkContext, dat1: String, dat2: String, stopwordsFile: String, goldStandardFile: String) {

  val amazonRDD: RDD[(String, String)] = Utils.getData(dat1, sc)
  val googleRDD: RDD[(String, String)] = Utils.getData(dat2, sc)
  val stopWords: Set[String] = Utils.getStopWords(stopwordsFile)
  val goldStandard: RDD[(String, String)] = Utils.getGoldStandard(goldStandardFile, sc)

  var amazonTokens: RDD[(String, List[String])] = _
  var googleTokens: RDD[(String, List[String])] = _
  var corpusRDD: RDD[(String, List[String])] = _
  var idfDict: Map[String, Double] = _

  var idfBroadcast: Broadcast[Map[String, Double]] = _
  var similarities: RDD[(String, String, Double)] = _

  def getTokens(data: RDD[(String, String)]): RDD[(String, List[String])] = {

    /*
     * getTokens soll die Funktion tokenize auf das gesamte RDD anwenden
     * und aus allen Produktdaten eines RDDs die Tokens extrahieren.
     */
    val stopWords_ = this.stopWords
    data.map(x => (x._1, EntityResolution.tokenize(x._2, stopWords_)))
  }

  def countTokens(data: RDD[(String, List[String])]): Long = {

    /*
     * Zählt alle Tokens innerhalb eines RDDs
     * Duplikate sollen dabei nicht eliminiert werden
     */

    data.map(_._2.size).sum.toLong
  }

  def findBiggestRecord(data: RDD[(String, List[String])]): (String, List[String]) = {

    /*
     * Findet den Datensatz mit den meisten Tokens
     */
    data.collect().maxBy(_._2.size)
  }

  def createCorpus = {

    /*
     * Erstellt die Tokenmenge für die Amazon und die Google-Produkte
     * (amazonRDD und googleRDD) und vereinigt diese und speichert das
     * Ergebnis in corpusRDD
     */
    corpusRDD = getTokens(amazonRDD).union(getTokens(googleRDD)).cache();
  }


  def calculateIDF = {

    /*
     * Berechnung des IDF-Dictionaries auf Basis des erzeugten Korpus
     * Speichern des Dictionaries in die Variable idfDict
     */

    def numOfDocWhereTokenAppearsIn(token: String, documentCorpus: RDD[(String, List[String])]): Long = {
      documentCorpus.filter(_._2.contains(token)).count()
    }

    def getUniqueTokens(c: RDD[(String, List[String])]): List[String] = {
      c.map(_._2).reduce((l, r) => l ::: r).distinct
    }
    val countOfAllDOcsInCorpusRDD = corpusRDD.count().toDouble
    val uniqueTokens = getUniqueTokens(corpusRDD)
    idfDict = uniqueTokens.map(x => (x, countOfAllDOcsInCorpusRDD / numOfDocWhereTokenAppearsIn(x, corpusRDD))).toMap
  }


  def simpleSimimilarityCalculation: RDD[(String, String, Double)] = {

    /*
     * Berechnung der Document-Similarity für alle möglichen
     * Produktkombinationen aus dem amazonRDD und dem googleRDD
     * Ergebnis ist ein RDD aus Tripeln bei dem an erster Stelle die AmazonID
     * steht, an zweiter die GoogleID und an dritter der Wert
     */
    val amazonRDD_ = this.amazonRDD
    val googleRDD_ = this.googleRDD
    val stopWords_ = this.stopWords
    val idfDict_ = this.idfDict
    val cartesianProduct = amazonRDD_.cartesian(googleRDD_)
    cartesianProduct.map(x => (x._1._1, x._2._1, EntityResolution.calculateDocumentSimilarity(x._1._2, x._2._2, idfDict_, stopWords_)))
  }

  def findSimilarity(vendorID1: String, vendorID2: String, sim: RDD[(String, String, Double)]): Double = {

    /*
     * Funktion zum Finden des Similarity-Werts für zwei ProduktIDs
     */
    sim.filter(x => vendorID1 == x._1 && vendorID2 == x._2).first()._3
  }

  def simpleSimimilarityCalculationWithBroadcast: RDD[(String, String, Double)] = {

    ???
  }

  /*
   *
   * 	Gold Standard Evaluation
   */

  def evaluateModel(goldStandard: RDD[(String, String)]): (Long, Double, Double) = {

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

object EntityResolution {

  def tokenize(s: String, stopws: Set[String]): List[String] = {
    /*
   	* Tokenize splittet einen String in die einzelnen Wörter auf
   	* und entfernt dabei alle Stopwords.
   	* Verwenden Sie zum Aufsplitten die Methode Utils.tokenizeString
   	*/
    Utils.tokenizeString(s).filter(!stopws.contains(_))
  }

  def getTermFrequencies(tokens: List[String]): Map[String, Double] = {

    /*
     * Berechnet die Relative Haeufigkeit eine Wortes in Bezug zur
     * Menge aller Wörter innerhalb eines Dokuments
     */
    tokens.map(x => (x, tokens.filter(_ == x).size.toDouble / tokens.size)).toMap
  }

  def computeSimilarity(record: ((String, String), (String, String)), idfDictionary: Map[String, Double], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem
     * Sie die erforderlichen Parameter extrahieren
     */
    ???
  }

  def calculateTF_IDF(terms: List[String], idfDictionary: Map[String, Double]): Map[String, Double] = {

    /*
     * Berechnung von TF-IDF Wert für eine Liste von Wörtern
     * Ergebnis ist eine Mapm die auf jedes Wort den zugehörigen TF-IDF-Wert mapped
     */

    getTermFrequencies(terms).map(x => (x._1, x._2 * idfDictionary.getOrElse(x._1, 0.0)))
  }

  //if dotProduct = 0 then both vectors are orthogonal angles
  def calculateDotProduct(v1: Map[String, Double], v2: Map[String, Double]): Double = {

    /*
     * Berechnung des Dot-Products von zwei Vectoren
     */
    v1.map(x => x._2 * v2.getOrElse(x._1, 0.0)).sum
  }

  def calculateNorm(vec: Map[String, Double]): Double = {

    /*
     * Berechnung der Norm eines Vectors
     */
    Math.sqrt(((for (el <- vec.values) yield el * el).sum))
  }

  def calculateCosinusSimilarity(doc1: Map[String, Double], doc2: Map[String, Double]): Double = {

    /*
     * Berechnung der Cosinus-Similarity für zwei Vectoren
     */
    calculateDotProduct(doc1, doc2) / (calculateNorm(doc1) * calculateNorm(doc2))
  }

  def calculateDocumentSimilarity(doc1: String, doc2: String, idfDictionary: Map[String, Double], stopWords: Set[String]): Double = {

    /*
     * Berechnung der Document-Similarity für ein Dokument
     */
    calculateCosinusSimilarity(calculateTF_IDF(tokenize(doc1, stopWords), idfDictionary), calculateTF_IDF(tokenize(doc2, stopWords), idfDictionary))
  }

  def computeSimilarityWithBroadcast(record: ((String, String), (String, String)), idfBroadcast: Broadcast[Map[String, Double]], stopWords: Set[String]): (String, String, Double) = {

    /*
     * Bererechnung der Document-Similarity einer Produkt-Kombination
     * Rufen Sie in dieser Funktion calculateDocumentSimilarity auf, in dem
     * Sie die erforderlichen Parameter extrahieren
     * Verwenden Sie die Broadcast-Variable.
     */
    ???
  }
}
