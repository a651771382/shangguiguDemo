package com.atguigu.hotitems_analysis

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

object kafkaProducer {
  def main(args: Array[String]): Unit = {
    writeKafka("hotitems")
  }

  def writeKafka(topic: String): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "192.168.159.131:9092")
    properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    //定义一个kafka properties
    val producer = new KafkaProducer[String, String](properties)
    //从文件中读取数据，发送
    val testPath = ""
    val bufferedSource = io.Source.fromFile(testPath)
    for (line <- bufferedSource.getLines()) {
      val record = new ProducerRecord[String, String](topic, line)
      producer.send(record)
    }
    producer.close()
  }
}
