package chapter04

object Test05_WhileLoop {
  def main(args: Array[String]): Unit = {
    //while
    var a: Int = 10
    while (a >= 1) {
      println("this is while loop: " + a)
      a -= 1
    }

    //do-while
    var b: Int = 0
    do {
      println("this is do while loop: s" + b)
      b -= 1
    } while (b > 0)
  }
}
