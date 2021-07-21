package com.atguigu.sinkTest

import com.atguigu.apiest.SourceTest.SensorReading
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer

object kafkaSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val inputStream = env.readTextFile("D:\\IDeaWarehouse\\shangguiguDemo\\flink_demo\\src\\main\\resources\\sensor.txt")


    val dataStream = inputStream.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble).toString //转成String方便序列化输出
    })

    dataStream.addSink(new FlinkKafkaProducer[String]("192.168.159.131:9092", "sinkTest", new SimpleStringSchema()))
    dataStream.print("kafkasink")

    env.execute("kafka sink test")
  }
}
