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
    def balance(chars: List[Char]): Boolean = ???
  
  /**
   * Exercise 3
   */
    def countChange(money: Int, coins: List[Int]): Int = ???
  }
