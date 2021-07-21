package com.atguigu.apiest

import com.atguigu.apiest.SourceTest.SensorReading
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.functions.{AssignerWithPeriodicWatermarks, AssignerWithPunctuatedWatermarks, KeyedProcessFunction}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object WindowTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.getConfig.setAutoWatermarkInterval(100L)
    //    val stream = env.readTextFile("D:\\IDeaWarehouse\\shangguiguDemo\\flink_demo\\src\\main\\resources\\sensor.txt")
    val stream = env.socketTextStream("192.168.159.131", 7777)

    val dataStream = stream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })
      //      .assignAscendingTimestamps(_.timestamp * 1000)
      //      .assignTimestampsAndWatermarks(new MyAssigner)
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
      override def extractTimestamp(t: SensorReading): Long = t.timestamp * 1000
    })

    //统计10秒内最小温度
    //    val minTemperWindowStream = dataStream
    //      .map(data => (data.id, data.timestamp))
    //      .keyBy(_._2)
    //      .timeWindow(Time.seconds(10)) //时间窗口
    //      .reduce((data1, data2) => {
    //      (data1._1, data1._2.min(data2._2)) //用reduce做增量聚合
    //    })
    //统计15内最小温度，搁5秒输出一次
    val minTemperWindowStream = dataStream
      .map(data => (data.id, data.timestamp))
      .keyBy(_._2)
      //      .window(SlidingEventTimeWindows.of(Time.seconds(15),Time.seconds(5),-8))
      .timeWindow(Time.seconds(10), Time.seconds(5)) //时间窗口
      .reduce((data1, data2) => {
      (data1._1, data1._2.min(data2._2)) //用reduce做增量聚合
    })
    minTemperWindowStream.print("min temp").setParallelism(1)
    dataStream.print("input data").setParallelism(1)

    dataStream.keyBy(_.id)
      .process(new MyProcess)

    env.execute("window test")
  }
}

class MyAssigner() extends AssignerWithPeriodicWatermarks[SensorReading] {
  val bound = 60000
  var maxTs = Long.MinValue

  override def getCurrentWatermark: Watermark = {
    new Watermark(1)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    maxTs = maxTs.max(t.timestamp * 1000)
    t.timestamp * 1000
  }
}

class MyAssigner1() extends AssignerWithPunctuatedWatermarks[SensorReading] {
  override def checkAndGetNextWatermark(t: SensorReading, l: Long): Watermark = {
    new Watermark(l)
  }

  override def extractTimestamp(t: SensorReading, l: Long): Long = {
    t.timestamp * 1000
  }
}

class MyProcess() extends KeyedProcessFunction[String, SensorReading, String] {
  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    context.timerService().registerEventTimeTimer(2000L)
  }
}
