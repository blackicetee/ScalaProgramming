package list

abstract class IntList {

  def isEmpty:Boolean
  def head:Int
  def tail:IntList
  def nth(index:Int):Int
  def contains(elem:Int):Boolean
  def insert(elem:Int):IntList
  def insertS(elem:Int):IntList
  def delete(elem:Int):IntList
  def deleteAll(elem:Int):IntList
	
  def insertSO(elem:Int):IntList= ???
  
  def insertionSort:IntList= ???

}