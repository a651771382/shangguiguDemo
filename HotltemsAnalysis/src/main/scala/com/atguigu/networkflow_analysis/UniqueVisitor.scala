package com.atguigu.networkflow_analysis

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.AllWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

case class UvCount(windowEnd: Long, uvCount: Long)

object UniqueVisitor {
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
      .timeWindowAll(Time.hours(1))
      .apply(new UvCountByWindow())


    env.execute()
  }
}

class UvCountByWindow() extends AllWindowFunction[UserBehavior, UvCount, TimeWindow] {
  override def apply(window: TimeWindow, input: Iterable[UserBehavior], out: Collector[UvCount]): Unit = {
    //定义一个scala set,用来保存所有的数据并去重
    var idSet = Set[Long]()
    //把当前窗口所有数据的id收集到set中，最后输出set的大小
    for (userBehavior <- input) {
      idSet += userBehavior.UserId
    }
    out.collect(UvCount(window.getEnd, idSet.size))
  }
}