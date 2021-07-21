package com.atguigu.apiest

import com.atguigu.apiest.SourceTest.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, RichMapFunction}
import org.apache.flink.streaming.api.scala._


object TranformTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val streamFromFlie: DataStream[String] = env.readTextFile("D:\\IDeaWarehouse\\shangguiguDemo\\flink_demo\\src\\main\\resources\\sensor.txt")
    //1.基本转换算子和简单聚合算子
    val dataStream: DataStream[SensorReading] = streamFromFlie.map(data => {
      val dataArray = data.split(",")
      SensorReading(dataArray(0).trim, dataArray(1).trim.toLong, dataArray(2).trim.toDouble)
    })
    val aggStream = dataStream.keyBy(0)
      //      .sum(2)
      //输出当前传感器最新的温度+10，而时间戳是上一次数据的时间+1
      .reduce((x, y) => SensorReading(x.id, x.timestamp + 1, x.temperature + 10))

    //2.多流转换算子
    //split分流
    val splitStream = dataStream.split(data => {
      if (data.temperature > 30) Seq("high") else Seq("low")
    })
    val high = splitStream.select("high")
    val low = splitStream.select("low")
    val all = splitStream.select("high", "low")
    //    high.print("high")
    //    low.print("low")
    //    all.print("all")
    //合并两条流
    val warning = high.map(data => (data.id, data.temperature))
    val connectedStream = warning.connect(low)

    val coMapDataStream = connectedStream.map(
      warningData => (warningData._1, warningData._2, "warning"),
      lowData => (lowData.id, lowData.temperature, "healthy")
    )
    coMapDataStream.print("coMapDataStream")

    val unionStream = high.union(low)
    unionStream.print("unionstream")

    dataStream.print().setParallelism(1)

    //函数类
    val filterStream: DataStream[SensorReading] = dataStream.filter(new MyFilter)
    filterStream.print("filterstream")

    env.execute("tranfrom test")
  }
}

class MyFilter() extends FilterFunction[SensorReading] {
  override def filter(t: SensorReading): Boolean = {
    t.id.startsWith("sensor_1")
  }
}

class MyMapper() extends RichMapFunction[SensorReading, String] {

  override def map(in: SensorReading): String = {
    "flink"
  }
}