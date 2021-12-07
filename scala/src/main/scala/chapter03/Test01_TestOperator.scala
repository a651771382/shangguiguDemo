package chapter03

object Test01_TestOperator {
  def main(args: Array[String]): Unit = {
    // 算数运算符
    val result1: Int = 10 / 3
    println(result1)

    val result2: Double = 10 / 3
    println(result2)

    val result3: Double = 10.0 / 3
    println(result3.formatted("%.2f"))

    // 比较运算符
    val s1: String = "Hello"
    val s2: String = new String("Hello")
    println(s1 == s2)
    println(s1.equals(s2))
    println(s1.eq(s2))
    println("===================================")

    //逻辑运行符
    def m(n: Int): Int = {
      println("m被调用")
      return n
    }

    val n = 1
    println((4 > 5) && m(n) > 0)
    println((4 < 5) && m(n) > 0)
    println((4 < 5) & m(n) > 0)

    //判断一个字符串是否为空
    def isNotEmpty(str: String): Boolean = {
      return str != null && !("".equals(str.trim))
    }

    println(isNotEmpty(null))

    println("=======================")
    //赋值计算符
    var b: Byte = 10
    var i: Int = 12
    //    b += 1
    i += 1
    println(i)

    //位运算符

    // 运算符的本质
    val n1: Int = 12
    val n2: Int = 37
    println(n1.+(n2))
    println(n1 + (n2))

    println(1.34.*(25))
    println(1.34 * 25)

    println(7.5 toString)
  }
}
