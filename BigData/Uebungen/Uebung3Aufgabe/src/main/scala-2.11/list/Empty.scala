package list

case object Empty extends IntList{
  def isEmpty=true
  def head:Int= throw new Error("head.nil")
  def tail:IntList= throw new Error("tail.nil")
  def contains(elem:Int):Boolean=false
  def nth(index:Int)= throw new Error("IndexOutOfBound")
  def insert(X:Int):IntList= new Cons(X,this)
  def insertS(elem:Int):IntList= ???
  def delete(elem:Int):IntList= ???
  def deleteAll(elem:Int):IntList = ???
}