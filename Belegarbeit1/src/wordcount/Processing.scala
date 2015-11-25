package wordcount

import mapreduce.BasicOperations

import scala.collection.immutable.::
import scala.util.matching.Regex
import scala.util.matching.Regex.MatchIterator

class Processing {

  /** ********************************************************************************************
    *
    * Aufgabe 1
    *
    * ********************************************************************************************
    */
  /*OLD IMPERATIVE IMPLEMENTATION FOR getWords()
      var wordList = List[String]()

    def reverseList[String](list: List[String]): List[String] = {
      def rlRec[String](result: List[String], list: List[String]): List[String] = {
        list match {
          case Nil => result
          case (x :: xs) => {
            rlRec(x :: result, xs)
          }
        }
      }
      rlRec(Nil, list)
    }

    def replaceNoneAsciiLetters(word: String): String = {
      val temp = word.replaceAll("[^a-zA-Z]", " ")
      temp.replaceAll("\\s+", " ")
    }

    val words = replaceNoneAsciiLetters(line).split(" ")
    for (word <- words) {
      wordList ::= word.toLowerCase()
    }
    reverseList(wordList)
   */
  def getWords(line: String): List[String] = {
    /*
     * Extracts all words in a line
     *
     * 1. Removes all characters which are not letters (A-Z or a-z)
     * 2. Shifts all words to lower case
     * 3. Extracts all words and put them into a list of strings
     */
    line.replaceAll("[^a-zA-Z]", " ").replaceAll("\\s+", " ").toLowerCase.split(" ").toList
  }

  def getAllWords(l: List[(Int, String)]): List[String] = {

    /*
     * Extracts all Words from a List containing tupels consisting
     * of a line number and a string
     * The Words should be in the same order as they occur in the source document
     *
     * Hint: Use the flatMap function
     */
    l.flatMap(x => getWords(x._2)).filter(_.nonEmpty)
  }


  /* OLD IMPERATIVE STYLE OF countTheWords()
      def getIndexWords(l: List[String]): List[String] = {
      def checkIfWordIsInList(word: String, list: List[String]): Boolean = {
        if (list.isEmpty) {
          false
        }
        else {
          list.contains(word)
        }
      }
      var indexList = List[String]()
      for (word <- l) {
        if (!checkIfWordIsInList(word, indexList)) {
          indexList ::= word
        }
      }
      indexList
    }
    var indexListTuple = List[(String, Int)]()
    val indexList = getIndexWords(l)
    for (indexWord <- indexList) {
      val pattern = new Regex("(\\(|\\s)" + indexWord + "(,|\\))")
      val temp = pattern.findAllIn(l.toString())
      indexListTuple ::=(indexWord, temp.length)
    }
    indexListTuple

   */
  def countTheWords(l: List[String]): List[(String, Int)] = {
    /*
     *  Gets a list of words and counts the occurences of the individual words
     */
    //l.groupBy(x => x) gibt eine Map[K, List(String)] zurück
    l.groupBy(x => x).map(t => (t._1, t._2.length)).toList
  }


  /** ********************************************************************************************
    *
    * Aufgabe 2
    *
    * ********************************************************************************************
    */

  def mapReduce[S, B, R](mapFun: (S => B),
                         redFun: (R, B) => R,
                         base: R,
                         l: List[S]): R =

    l.map(mapFun).foldLeft(base)(redFun)

  def countTheWordsMR(l: List[String]): List[(String, Int)] = {
    def insertL(l: List[(String, Int)], el: (String, Int)): List[(String, Int)] = l match {
      case Nil => List(el)
      case x :: xs if (el._1.equals(x._1)) => (el._1, el._2 + x._2) :: xs
      case x :: xs => x :: insertL(xs, el)
    }
    mapReduce[String, (String, Int), List[(String, Int)]](X=>(X,1), insertL, List(), l)
  }


  /** ********************************************************************************************
    *
    * Aufgabe 3
    *
    * ********************************************************************************************
    */

  def getAllWordsWithIndex(l: List[(Int, String)]): List[(Int, String)] = {
    /*
   * Extracts all Words from a List containing tupels consisting
   * of a line number and a string
   */
    //l.flatMap(x=>getWords(x._2).map(y=>(x._1, y)))
    var indexList = List[(Int, String)]()
    for (element <- l) {
      if (!element._2.isEmpty) {
        for (word <- getWords(element._2)) {
          indexList ::=(element._1, word)
        }
      }
    }
    indexList
  }

  def createInverseIndex(l: List[(Int, String)]): Map[String, List[Int]] = {
    var inverseIndex = Map[String, List[Int]]()
    for (elem <- getAllWordsWithIndex(l)) {
      if (inverseIndex.contains(elem._2) && !inverseIndex(elem._2).contains(elem._1)) {
        var lineNrs = inverseIndex(elem._2) :+ elem._1
        inverseIndex += elem._2 -> lineNrs
      }
      else if (!inverseIndex.contains(elem._2)) {
        var lineNrs = List[Int](elem._1)
        inverseIndex += elem._2 -> lineNrs
      }
    }
    inverseIndex
  }

  //val c = words.map(s=> invInd.getorElse(s, Listempty))
  //c.fold(c.head)((list, x) => list.(intersect(x))
  def andConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {
    var lineNrs = List[Int]()
    var validLineNrs = List[Int]()
    for (word <- words) {
      if (invInd.contains(word)) {
        if (lineNrs.isEmpty && validLineNrs.isEmpty) {
          lineNrs = invInd(word)
          validLineNrs = invInd(word)
        } else if (!lineNrs.isEmpty && validLineNrs.isEmpty) {
          for (word <- words) {
            if (!lineNrs.contains(word)) {
              lineNrs :+ word
              validLineNrs :+ word
            }
          }
        } else {
          for (lineNr <- validLineNrs) {
            if (!invInd(word).contains(lineNr)) {
              validLineNrs = validLineNrs.filter(_ != lineNr)
            }
          }
        }
      } else {
        return List[Int]()
      }
    }
    validLineNrs
  }

  //words.flatmap(x=>invInd.getOrElse(x, List())
  def orConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {
    var validLineNrs = List[Int]()
    for (word <- words) {
      if (invInd.contains(word)) {
        if (validLineNrs.isEmpty) {
          validLineNrs = invInd(word)
        } else {
          for (word <- words) {
            if (!validLineNrs.contains(word)) {
              validLineNrs :+ word
            }
          }
        }
      }
    }
    validLineNrs
  }


  /** ********************************************************************************************
    *
    * Aufgabe 4
    *
    * ********************************************************************************************
    */


  //((x) => {getWords(x._2).map(y=>(y, 1))}
  //(y) => {
  //List((y._1, (y._2 foldLeft(o)(_+_))
  //...
  def countTheWordsMRGoogle(text: List[(Int, String)]): List[(String, Int)] = {
    val mapFun = (keyAndValue: (String, Int)) => {
      var mappedList = List[(String, Int)]()
      if (!keyAndValue._1.isEmpty) {
        val t = keyAndValue._1.toLowerCase.replaceAll("[^a-z]", " ")
        val temp = t.replaceAll("\\s+", " ")
        val wl = temp.split(" ").toList
        for (word <- wl) {
          mappedList ::=(word, keyAndValue._2)
        }
      }
      mappedList
    }

    val redFun = (keysAndValues: (String, List[Int])) => {
      def recursiveSumListElements(l: List[Int], count: Int): Int = l match {
        case Nil => count
        case x :: xs => recursiveSumListElements(xs, count + 1)
      }
      List((keysAndValues._1, recursiveSumListElements(keysAndValues._2, 0)))
    }

    val data = (text: List[(Int, String)], valueIn: Int) => {
      var finalData = List[(String, Int)]()
      for (row <- text) {
        finalData ::=(row._2, valueIn)
      }
      finalData
    }

    BasicOperations.mapReduce[String, Int, String, Int, String, Int](mapFun, redFun, data(text, 1))
  }
}


object Processing {

  def getData(filename: String): List[(Int, String)] = {

    val url = getClass.getResource("/" + filename).getPath
    val src = scala.io.Source.fromFile(url)
    val iter = src.getLines()
    var c = -1
    val result = (for (row <- iter) yield {
      c = c + 1; (c, row)
    }).toList
    src.close()
    result
  }
}