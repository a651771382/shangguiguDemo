package com.atguigu.loginfail_detect

import java.util

import org.apache.flink.cep.PatternSelectFunction
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

object LoginFailWithCep {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //读取事件数据,创建简单事件流
    val resource = getClass.getResource("")
    val loginEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        LoginEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(5)) {
        override def extractTimestamp(element: LoginEvent): Long = element.enentTime * 1000L
      })

    val warningStream = loginEventStream
      .keyBy(_.userId) //以用户id做分组

    //定义匹配模式
    val loginFailPattern = Pattern.begin[LoginEvent]("begin").where(_.eventType == "fail")
      .next("next").where(_.enentTime == "fail")
      .within(Time.seconds(2))

    //在事件流上应用模式，得到一个patten stream
    val pattenStream = CEP.pattern(loginEventStream, loginFailPattern)

    //从patten stream上应用select function，检出匹配事件序列
    val loginFailDataStream = pattenStream.select(new LoginFailMatch())
    loginFailDataStream.print()

    env.execute()
  }
}

class LoginFailMatch() extends PatternSelectFunction[LoginEvent, Waring] {
  override def select(map: util.Map[String, util.List[LoginEvent]]): Waring = {
    //从map中按照名称，取出对应的事件
    val firstFail = map.get("begin").iterator().next()
    val lastFail = map.get("next").iterator().next()
    Waring(firstFail.userId, firstFail.enentTime, lastFail.enentTime, "Login fail")
  }
}