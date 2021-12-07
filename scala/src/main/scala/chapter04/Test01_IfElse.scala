package chapter04

import scala.io.StdIn

object Test01_IfElse {
  def main(args: Array[String]): Unit = {
    println("请输入您的年龄")
    val age: Int = StdIn.readInt()

    //单分支
    if (age >= 18) {
      println("成年")
    }

    println("============")

    //双分支
    if (age >= 18)
      println("成年")
    else
      println("未成年")

    println("==================")

    //多分支
    if (age <= 6)
      println("童年")
    else if (age < 18)
      println("青少年")
    else if (age < 35)
      println("青年")
    else if (age < 60)
      println("中年")
    else
      println("老年")


    //分支语句的返回值
    val result: Any = if (age <= 6) {
      println("童年")
      "童年"
    }
    else if (age < 18) {
      println("青少年")
      "青少年"
    }
    else if (age < 35) {
      println("青年")
      age
    }
    else if (age < 60) {
      println("中年")
      "中年"
    }
    else {
      println("老年")
      "老年"
    }

    println("result:" + result)

    // java中三元运算符
    val res = if (age >= 18) {
      println("成年")
    } else {
      println("未成年")
    }

    val res1 = if (age >= 18) "成年" else "未成年"

    //嵌套分支
    if (age >= 18) {
      println("成年")
      if (age >= 35) {
        println()
        if (age >= 60) {
          println("老年")
        } else {
          println("中年")
        }
      } else {
        println("青年")
      }
    } else {
      println("未成年")
      if (age >= 6) {
        println("童年")
      } else {
        println("青少年")
      }
    }
  }
}
