package com.atguigu.apiest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

import scala.util.Random


object SourceTest {
  def main(args: Array[String]): Unit = {
    //创建程序入口
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //1.从自定义的集合中读取数据
    val stream1 = env.fromCollection(List(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718201, 15.402984393403084),
      SensorReading("sensor_7", 1547718202, 6.720945201171228),
      SensorReading("sensor_10", 1547718205, 38.1010676048993444)
    ))
    stream1.print("stream1").setParallelism(1)
    //2.从文件中读取文件
    val stream2 = env.readTextFile("D:\\IDeaWarehouse\\shangguiguDemo\\flink_demo\\src\\main\\resources\\sensor.txt")
    stream2.print("stream2").setParallelism(1)

    //3.从kafka中读取数据
//    val properties = new Properties()
//    properties.setProperty("bootstrap.servers", "192.168.159.131:9092")
//    properties.setProperty("group.id", "consumer-group")
//    properties.setProperty("zookeeper.connect", "192.168.159.131:2181,192.168.159.132:2191,192.168.159.128:2181")
//    properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
//    properties.setProperty("auto.offset.reset", "latest")
    //    val stream3 = env.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema, properties))
    //    stream3.print("stream3").setParallelism(1)

    //4.自定义source
    val stream4 = env.addSource(new SensorSource())
    stream4.print("stream4").setParallelism(1)

    env.execute("source test")
  }

  //温度传感器样例类
  case class SensorReading(id: String, timestamp: Long, temperature: Double)

  class SensorSource() extends SourceFunction[SensorReading] {
    //定义一个flag,表示数据源是否正常运行
    var running: Boolean = true

    //正常生成数据
    override def run(ctx: SourceFunction.SourceContext[SensorReading]): Unit = {
      //初始化一个随机数发生器
      val rand = new Random()
      //初始化定义一组传感器温度数据
      var curTmp = 1.to(10).map(
        i => ("sensor_" + i, 60 + rand.nextGaussian() * 20)
      )

      //用无限循环.产生数据流
      while (running) {
        //在前一次温度的基础上更新温度值
        curTmp = curTmp.map(
          t => (t._1, t._2 + rand.nextGaussian())
        )
        //获取当前时间戳
        val curTime = System.currentTimeMillis()
        curTmp.foreach(
          t => ctx.collect(SensorReading(t._1, curTime, t._2))
        )
        //设置时间间隔
        Thread.sleep(500)
      }
    }

    //取消数据源的生成
    override def cancel(): Unit = {
      running = false
    }
  }

}
