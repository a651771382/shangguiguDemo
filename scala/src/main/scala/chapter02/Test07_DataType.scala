package chapter02

import chapter01.Student

object Test07_DataType {
  def main(args: Array[String]): Unit = {

    //整数类型
    val a1: Byte = 127
    val a2: Byte = -128
    // val a2: Byte = 128  //error

    val a3 = 12 //默认类型为Int
    val a4 = 12346549875456123L //长整数值定义

    val b1: Byte = 10
    val b2: Byte = (10 + 20)
    println(b2)

    //    val b3:Byte = (b2+20)
    val b3: Byte = (b2 + 20).toByte
    println(b3)

    // 浮点类型
    val f1: Float = 1.216f
    val d1 = 1231.5645

    //字符类型
    val c1: Char = 'a'
    println(c1)

    val c2: Char = '9'
    println(c2)

    val c3: Char = '\t' // 指标符
    val c4: Char = '\n' // 跨行符
    println("abc" + c3 + "dsfa")
    println("avds" + c4 + "dsaf")

    val c5 = '\\' //表示\本身
    val c6 = '\"' //表示\本身
    println("abc" + c5 + "dsfa")
    println("abc" + c6 + "dsfa")

    // 字符变量底层保存的是ASCII码
    val i1: Int = c1
    println("i1:" + i1)
    val i2: Int = c2
    println("i2:" + i2)

    val c7: Char = (i1 + 1).toChar
    println("i7:" + c7)

    val c8: Char = (i2 + 1).toChar
    println("i8:" + c8)

    val c9: Char = (i2 - 1).toChar
    println("i9:" + c9)

    // 布尔类型
    val isTure: Boolean = true
    println(isTure)

    // 空类型
    // 空值null
    def m1(): Unit = {
      println("m1被调用执行")
    }

    val a: Unit = m1()
    println("a:  " + a)

    // 空引用Null
    //    val n:Int = null //error
    var student = new Student("alice", 20)
    student = null
    println(student)

    // Nothing
    def m2(n: Int): Int = {
      if (n == 0)
        throw new NullPointerException
      else
        return n


    }

    val b = m2(2)
    println("b的值： "+b)
  }
}
