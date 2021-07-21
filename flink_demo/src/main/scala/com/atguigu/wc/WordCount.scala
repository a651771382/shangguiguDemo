package com.atguigu.wc

import org.apache.flink.api.scala._


object WordCount {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    //读取文件
    val inputFile = env.readTextFile("D:\\IDeaWarehouse\\shangguiguDemo\\flink_demo\\src\\main\\resources\\wc.csv")
    val word = inputFile.flatMap(_.split(" ")).map((_, 1))
      .groupBy(0).sum(1)
    word.print()
  }
}
