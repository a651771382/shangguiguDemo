package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//先定义输入数据的样例类
case class UserBehavior(UserId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)

object PageView {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //用相对路径定义数据源
    val txtPath = ""
    val resource = getClass.getResource(txtPath)
    val dataStream = env.readTextFile(resource.getPath)
      .map(data => {
        val strings: Array[String] = data.split(",")
        UserBehavior(strings(0).trim.toLong, strings(1).trim.toLong, strings(2).trim.toInt, strings(3).trim, strings(4).trim.toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior == "pv")
      .map(data => {
        ("pv", 1)
      })
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .sum(1)
    dataStream.print("pv count")

    env.execute()
  }
}
