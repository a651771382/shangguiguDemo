package chapter02

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
  }
}
