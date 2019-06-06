package recfun

object Main {
  def main(args: Array[String]) {
    println("Pascal's Triangle")
    for (row <- 0 to 10) {
      for (col <- 0 to row)
        print(pascal(col, row) + " ")
      println()
    }
  }

  /**
   * Exercise 1
   */
    def pascal(c: Int, r: Int): Int = {
      // if requested element is in first column or first row or second row or last column, then return 1
      if (c == 0 | r == 0 | r == 1 | c == r) {
        1
      }else{
        // else go to the previous row, and add the (n-1)th column and nth column. 
        pascal(c-1, r-1) + pascal(c,r-1)
      }
    }
  
  /**
   * Exercise 2
   */
    def balance(chars: List[Char]): Boolean = {
      def isBalanced(stack: List[Char], chars: List[Char]): Boolean = {
        // if end of line is reached and stack is empty, return true
        if (chars.isEmpty && stack.isEmpty) {
          return true
        } else if (chars.isEmpty && stack.nonEmpty) {
          // else if end of line is reached and stack is still not empty, return false
          return false
        }
        // else end of line not reached, so continue traversing
        val curr = chars.head
        val nextList = chars.tail

        // if curr is ), check stack.
        if (curr == ')') {
          // if stack is empty (i.e. no floating parentheses), return false
          if (stack.isEmpty) {
            return false
          } else if (stack.head == '(') { // if stack is non-empty, pop it and continue
            isBalanced(stack.tail, nextList)
          } else {
            // if curr is any other character, just continue traversing
            isBalanced(stack, nextList)
          }
        } else if (curr == '(') { // if curr is (, just add to stack and continue
          isBalanced(curr +: stack, nextList)
        } else { // if curr is any other character, just continue traversing
          isBalanced(stack, nextList)
        }
      }
      isBalanced(List.empty[Char],chars)
    }
  
  /**
   * Exercise 3
   */
    def countChange(money: Int, coins: List[Int]): Int = ???
  }
