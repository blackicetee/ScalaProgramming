package Basics

/**
 * Created by tstegemann on 01.12.2015.
 */
object ValidateParenthesis {
  def main(args: Array[String]) {
    println(balance(")(((there are no brackets)))(".toCharArray.toList))
  }
  def balance(chars: List[Char]): Boolean = {
    def helper(balValue: Int, chars: List[Char]): Boolean = (balValue, chars) match {
      case (value, Nil) => (0 == value)
      case (value, x::xs) => if (x == '(') helper(value + 1, xs) else {
        if (x == ')' && value < 1) false else if (x == ')') helper(value - 1, xs) else helper(value, xs)
      }
    }
    helper(0, chars)
  }
}
