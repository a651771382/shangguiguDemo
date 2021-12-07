package chapter02

object Test08_DataTypeConversion {
  def main(args: Array[String]): Unit = {
    // 自动类型转换
    //    （1）自动提升原则：有多种类型的数据混合运算时，系统首先自动将所有数据转换成精度大的那种数据类型，然后再进行计算。
    val a1: Byte = 10
    val b1: Long = 23565L
    val result1: Long = (a1 + b1)
    val result2: Int = (a1 + b1.toInt) // 强转

    //    （2）把精度大的数值类型赋值给精度小的数值类型时，就会报错，反之就会进行自动类型转换。
    val a2: Byte = 10
    val b2: Int = a2
    //    val c2:Byte = b2  // error

    //    （3）（byte，short）和 char 之间不会相互自动转换。
    val a3: Byte = 10;
    val b3: Char = 'b'
    //    val c3: Byte = b3  // error
    val c3: Int = b3
    println(c3)
    //    （4）byte，short，char 他们三者可以计算，在计算时首先转换为 int 类型。
    val a4: Byte = 10;
    val b4: Short = 25
    val c4: Char = 'c'
    val result4: Int = a4 + b4
    val result44: Int = a4 + b4 + c4
    println(result44)

    //强制类型转换
    //    （1）基本类型转 String 类型（语法：将基本类型的值+"" 即可）
    val n1: Int = -2.9.toInt
    println(n1)
    //    （2）String 类型转基本数值类型（语法：s1.toInt、s1.toFloat、s1.toDouble、s1.toByte、s1.toLong、s1.toShort）
    val n2: Int = (2.6 + 3.7).toInt

    // 数值类型和String类型转换
    // 数值转String
    val n: Int = 27
    val s: String = n + ""

    // String转数值
    val m: Int = "12".toInt
    val f: Float = "12.3".toFloat
    val f2: Int = "12.3".toDouble.toInt

  }
}
