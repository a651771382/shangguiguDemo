package com.atguigu.order_detect


import java.util

import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

//输入订单事件样例类
case class OrderEvent(orderId: Long, eventType: String, txId: String, eventTime: Long)

//输出结果样例类
case class OrderRresult(orderId: Long, resultMsg: String)

object OrderTimeout {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    //读取订单数据
    val resource = getClass.getResource("")
    val orderEventStream = env.readTextFile(resource.getPath)
      .map(data => {
        val dataArray = data.split(",")
        OrderEvent(dataArray(0).trim.toLong, dataArray(1).trim, dataArray(2).trim, dataArray(3).trim.toLong)
      })
      .assignAscendingTimestamps(_.eventTime)
      .keyBy(_.orderId)

    //定义一个匹配模式
    val orderPattern = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create")
      .followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))

    //把模式应用到Stream上，得到一个pattern Stream
    val patternStream = CEP.pattern(orderEventStream, orderPattern)

    //调用select方法，提取事件序列,超时的事件要做报警提示
    val orderTimeoutOutpuTag = new OutputTag[OrderRresult]("orderTimeout")

    val resultStream = patternStream.select(orderTimeoutOutpuTag, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.print("payed")
    resultStream.getSideOutput(orderTimeoutOutpuTag).print("timeout")

    env.execute()
  }
}

//自定义超时处理函数
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent, OrderRresult] {
  override def timeout(map: util.Map[String, util.List[OrderEvent]], l: Long): OrderRresult = {
    val timeoutOrderId = map.get("begin").iterator().next().orderId
    OrderRresult(timeoutOrderId, "timeout")
  }
}

//自定义正常支付事件序列处理函数
class OrderPaySelect() extends PatternSelectFunction[OrderEvent, OrderRresult] {
  override def select(map: util.Map[String, util.List[OrderEvent]]): OrderRresult = {
    val payedorderId = map.get("follow").iterator().next().orderId
    OrderRresult(payedorderId, "payed successfully")
  }
}