package chapter04

import scala.collection.immutable

object Test02_ForLoop {
  def main(args: Array[String]): Unit = {
    // java for 语法： for(int i =0;i<10;i++){System.out.println("Hello world")}
    // 范围遍历
    for (i <- 1 to 10) {
      println(i + "Hello world")
    }
    println("===============")
    for (i <- 1.to(10)) {
      println(i + "Hello world")
    }
    println("=================")
    for (i <- Range(1, 10)) {
      println(i + "Hello world")
    }
    println("=================")
    for (i <- 1 until 10) {
      println(i + "Hello world")
    }

    // 集合遍历
    println("=================")
    for (i <- Array(12, 4564, 897, 45, 45)) {
      println(i)
    }
    println("=================")
    for (i <- List(12, 4564, 897, 45, 45)) {
      println(i)
    }
    println("=================")
    for (i <- Set(12, 4564, 897, 45, 45)) {
      println(i)
    }
    println("=================")
    //循环守卫
    for (i <- 1 to 10) {
      if (i != 5) {
        println(i)
      }
    }
    println("=================")
    for (i <- 1 to 10 if i != 5) {
      println(i)
    }

    println("=================")
    //循环步长
    for (i <- 1 to 10 by 2) {
      println(i)
    }
    println("-------------------------")
    for (i <- 13 to 30 by 3) {
      println(i)
    }

    println("-------------------------")
    for (i <- 13 to 3 by -2) {
      println(i)
    }

    println("-------------------------")
    for (i <- 1 to 10 reverse) {
      println(i)
    }

    //    for (i <- 1 to 10 by 0) {
    //      println(i)
    //    } // error ,step不能为0
    println("-------------------------")
    for (i <- 1.0 to 10.0 by 0.5) {
      println(i)
    }
    println("++++++++++++++++++++++++++")
    // 循环嵌套
    for (i <- 1 to 3) {
      for (j <- 1 to 3) {
        println("i=" + i + ", j=" + j)
      }
    }
    println("=====================")
    for (i <- 1 to 3; j <- 1 to 6) {
      println("i=" + i + ", j=" + j)
    }

    // 循坏引入变量
    for (i <- 1 to 10) {
      val j = 10 - i
      println("i=" + i + ", j=" + j)
    }

    for (i <- 1 to 10; j = 10 - i) {
      println("i=" + i + ", j=" + j)
    }

    for {
      i <- 1 to 10
      j = 10 - i
    } {
      println("i=" + i + ", j=" + j)
    }

    // 循环返回值
    val a: Unit = for (i <- 1 to 10) {
      println(i)
    }
    println("a = " + a)

    val b: immutable.IndexedSeq[Int] = for (i <- 1 to 10) yield i
    println("b = " + b)

    val b1: immutable.IndexedSeq[Int] = for (i <- 1 to 17) yield i * i
    println("b1 = " + b1)
  }
}
