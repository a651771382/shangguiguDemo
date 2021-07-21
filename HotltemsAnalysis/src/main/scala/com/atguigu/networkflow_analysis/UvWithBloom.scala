package com.atguigu.networkflow_analysis


import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import redis.clients.jedis.Jedis

object UvWithBloom {
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
      .map(data => ("dummykey", data.UserId))
      .keyBy(_._1)
      .timeWindow(Time.hours(1))
      .trigger(new MyTrigger())
      .process(new UcCountWithBloom())

    env.execute()
  }
}

class MyTrigger() extends Trigger[(String, Long), TimeWindow] {
  override def onElement(element: (String, Long), timestamp: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onProcessingTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = TriggerResult.CONTINUE

  override def onEventTime(time: Long, window: TimeWindow, ctx: Trigger.TriggerContext): TriggerResult = {
    //每来一条数据，就直接触发窗口操作，并清空所有窗口状态
    TriggerResult.FIRE_AND_PURGE
  }

  override def clear(window: TimeWindow, ctx: Trigger.TriggerContext): Unit = {}
}

//定义一个布隆过滤器
class Bloom(size: Long) extends Serializable {
  //位图的总大小
  private val cap = if (size > 0) size else 1 << 27

  //定义hash函数
  def hash(value: String, seed: Int): Long = {
    var result: Long = 0L
    for (i <- 0 until value.length) {
      result = result * seed + value.charAt(i)
    }
    result & (cap - 1)
  }
}

class UcCountWithBloom() extends ProcessWindowFunction[(String, Long), UvCount, String, TimeWindow] {
  //定义redis连接
  lazy val jedis = new Jedis("localhost", 6379)
  lazy val bloom = new Bloom(1 << 29)

  override def process(key: String, context: Context, elements: Iterable[(String, Long)], out: Collector[UvCount]): Unit = {
    //位图的存储方式，key是windowEnd,value是bitmap
    val storekey = context.window.getEnd.toString
    var count = 0L
    //把每个窗口的uv count值存入redis中，存放内容为（windowEnd -》uvCount）所以要先从redis读取
    if (jedis.hget("count", storekey) != null) {
      count = jedis.hget("count", storekey).toLong
    }
    //用布隆过滤器判断当前用户是否已存在
    val userId = elements.last._2.toString
    val offset = bloom.hash(userId, 61)
    //定义一个标识位，判断redis位图中有没有这一位
    val idExist = jedis.getbit(storekey, offset)
    if (!idExist) {
      //如果不存在，位图对应位置1，count+1
      jedis.setbit(storekey, offset, true)
      jedis.hset("count", storekey, (count + 1).toString)
      out.collect(UvCount(storekey.toLong, count + 1))
    } else {
      out.collect(UvCount(storekey.toLong, count))
    }
  }
}