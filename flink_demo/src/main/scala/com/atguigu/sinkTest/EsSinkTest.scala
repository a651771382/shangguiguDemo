package com.atguigu.sinkTest

import java.util

import com.atguigu.apiest.SourceTest.SensorReading
import org.apache.flink.api.common.functions.RuntimeContext
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.elasticsearch.{ElasticsearchSinkFunction, RequestIndexer}
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink
import org.apache.http.HttpHost
import org.elasticsearch.client.Requests

object EsSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //source
    val inputStream = env.readTextFile("")
    //tranform
    val dataStream = inputStream.map(data => {
      val strings: Array[String] = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })

    val httpHost = new util.ArrayList[HttpHost]()
    httpHost.add(new HttpHost("localhost", 9200))
    //创建一个esSink的builder
    val esSinkBuilder = new ElasticsearchSink.Builder[SensorReading](
      httpHost,
      new ElasticsearchSinkFunction[SensorReading] {
        override def process(t: SensorReading, runtimeContext: RuntimeContext, requestIndexer: RequestIndexer): Unit = {
          println("saving data:" + t)
          //包装成一个map或者JSonObject
          val json = new util.HashMap[String, String]()
          json.put("sensor_id", t.id)
          json.put("temperature", t.temperature.toString())
          json.put("timestamp", t.timestamp.toString)

          //创建index request，准备发送数据
          val indexRequest = Requests.indexRequest()
            .index("sensor")
            .`type`("readingdata")
            .source(json)

          //利用index发送请求，写入数据
          requestIndexer.add(indexRequest)
          println("data saved")
        }
      })

    //sink
    dataStream.addSink(esSinkBuilder.build())

    env.execute("es sink test")
  }
}
