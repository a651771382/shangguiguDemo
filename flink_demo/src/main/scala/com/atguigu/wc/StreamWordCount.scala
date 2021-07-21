package com.atguigu.wc

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //禁止合并任务链 在算子后使用的disableChaining()函数禁止，禁止算子后(包括当前算子)的任务链合并
    //    env.disableOperatorChaining()
    //startNewChain()开启新的任务链
    val dateStream = env.socketTextStream("192.168.159.131", 7777)
    val wordCount = dateStream.flatMap(_.split(" "))
      .map((_, 1))
      .keyBy(0)
      .sum(1)
    wordCount.print()
    //启动executor
    env.execute("stream word count job")
  }
}
