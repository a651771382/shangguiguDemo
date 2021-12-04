package chapter01

/*
 object 是一个关键字，声明一个单利对象
 */
object HelloWord {
  /**
   *
   * mian 方法：从外部可以直接调用执行的方法
   * def 方法名称（参数名称：参数类型）：返回值类型 = {方法体}
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    print("hello word")
    System.out.println("hello scala from java")
  }
}
