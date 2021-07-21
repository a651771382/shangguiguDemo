package com.atguigu.order_detect

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object OrderTimeoutWithoutCep {
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

    //定义process function进行超时检测
    val timeoutWarningStream = orderEventStream.process(new OrderTimeoutWarning())

    timeoutWarningStream.print()

    env.execute()
  }
}

//实现自定义的处理函数
class OrderTimeoutWarning() extends KeyedProcessFunction[Long, OrderEvent, OrderRresult] {
  //保存pay支付事件是否来过
  lazy val isPayedState: ValueState[Boolean] = getRuntimeContext.getState(new ValueStateDescriptor[Boolean]("ispayed-state", classOf[Boolean]))

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, OrderEvent, OrderRresult]#OnTimerContext, out: Collector[OrderRresult]): Unit = {
    //判断ispayed的状态是否为true
    val ispayed = isPayedState.value()
    if (ispayed) {
      out.collect(OrderRresult(ctx.getCurrentKey, "order payed successfully"))
    } else {
      out.collect(OrderRresult(ctx.getCurrentKey, "order timeout"))
    }
    //清空状态
    isPayedState.clear()
  }

  override def processElement(value: OrderEvent, ctx: KeyedProcessFunction[Long, OrderEvent, OrderRresult]#Context, out: Collector[OrderRresult]): Unit = {
    //先取出状态标识位
    val ispayed = isPayedState.value()
    if (value.eventType == "create" && !ispayed) {
      //如果遇到了create事件，并且pay没有来过，注册定时器开始等待
      ctx.timerService().registerEventTimeTimer(value.eventTime * 1000L + 15 * 60 * 1000L)
    } else if (value.eventType == "pay") {
      //如果是pay事件，直接把状态改为true
      isPayedState.update(true)
    }

  }
}