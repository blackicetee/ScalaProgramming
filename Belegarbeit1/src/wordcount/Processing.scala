package wordcount

import common._
import mapreduce.BasicOperations

import scala.util.matching.Regex
import scala.util.matching.Regex.MatchIterator

class Processing {

  /** ********************************************************************************************
    *
    * Aufgabe 1
    *
    * ********************************************************************************************
    */
  def getWords(line: String): List[String] = {
    /*
     * Extracts all words in a line
     * 
     * 1. Removes all characters which are not letters (A-Z or a-z)
     * 2. Shifts all words to lower case
     * 3. Extracts all words and put them into a list of strings
     */
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
  }

  def getAllWords(l: List[(Int, String)]): List[String] = {

    /*
     * Extracts all Words from a List containing tupels consisting
     * of a line number and a string
     * The Words should be in the same order as they occur in the source document
     * 
     * Hint: Use the flatMap function
     */
    val result = l.flatMap(x => getWords(x._2))
    result.filter(_.nonEmpty)
  }

  def countTheWords(l: List[String]): List[(String, Int)] = {

    /*
     *  Gets a list of words and counts the occurences of the individual words
     */
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
      val pattern = new Regex(indexWord)
      val temp = pattern.findAllIn(l.toString())
      indexListTuple ::= (indexWord, 1)
    }
    val i = indexListTuple
    Nil
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

    def countTheWordsMR(l: List[String]): List[(String, Int)] = ???


    /** ********************************************************************************************
      *
      * Aufgabe 3
      *
      * ********************************************************************************************
      */

    def getAllWordsWithIndex(l: List[(Int, String)]): List[(Int, String)] = ???

    /*
     * Extracts all Words from a List containing tupels consisting
     * of a line number and a string
     */


    def createInverseIndex(l: List[(Int, String)]): Map[String, List[Int]] = {

      ???
    }

    def andConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {

      ???
    }

    def orConjunction(words: List[String], invInd: Map[String, List[Int]]): List[Int] = {

      ???
    }


    /** ********************************************************************************************
      *
      * Aufgabe 4
      *
      * ********************************************************************************************
      */


    def countTheWordsMRGoogle(text: List[(Int, String)]): List[(String, Int)] = ???

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