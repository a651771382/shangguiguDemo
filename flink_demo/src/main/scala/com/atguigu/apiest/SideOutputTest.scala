package com.atguigu.apiest

import com.atguigu.apiest.SourceTest.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object SideOutputTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.socketTextStream("192.168.159.131", 7777)

    val dataStream = stream.map(data => {
      val strings = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(1)) {
        override def extractTimestamp(t: SensorReading): Long = {
          t.timestamp * 1000
        }
      })

    val processedStream = dataStream
      .process(new FreezingAlert())

    processedStream.print("process")
    processedStream.getSideOutput(new OutputTag[String]("freezing alert")).print("alert data")

    env.execute("side out put test")
  }
}

//冰点温度报警，如果小于32华摄氏度，输出报警信息到侧输出流
class FreezingAlert() extends ProcessFunction[SensorReading, SensorReading] {
  lazy val alertOutput = new OutputTag[String]("freezing alert")

  override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
    if (i.temperature < 32.0) {
      context.output(alertOutput, "freezing alert" + i.id)
    } else {
      collector.collect(i)
    }
  }
}