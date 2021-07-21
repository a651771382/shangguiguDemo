package com.atguigu.apiest

import java.util.Properties

import org.apache.flink.streaming.api.scala._
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

object kafkaSourTest {
  def main(args: Array[String]): Unit = {
    //创建程序入口
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    val properties = new Properties()
    properties.setProperty("bootstrap.servers", "192.168.159.131:9092")
    properties.setProperty("group.id", "consumer-group1")
    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "earliest")

    val stream3 = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema, properties))
    stream3.print("stream3").setParallelism(1)
    env.execute()
  }
}
