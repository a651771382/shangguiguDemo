package com.atguigu.sinkTest

import java.sql.{Connection, Driver, DriverManager, PreparedStatement}

import com.atguigu.apiest.SourceTest.SensorReading
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object JDBCSinkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //source
    val inputStream = env.readTextFile("")
    //tranform
    val dataStream = inputStream.map(data => {
      val strings: Array[String] = data.split(",")
      SensorReading(strings(0).trim, strings(1).trim.toLong, strings(2).trim.toDouble)
    })

    //sink
    dataStream.addSink(new MyJDBCSink())

    env.execute("jdbc sink test")
  }
}

class MyJDBCSink() extends RichSinkFunction[SensorReading] {
  //定义sql的连接、预编译器
  var conn: Connection = _
  var insertStream: PreparedStatement = _
  var updateStream: PreparedStatement = _

  //初始化，创建连接和预编译语句
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
    conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/test", "root", "123456")
    insertStream = conn.prepareStatement("insert into temperatures (sensor,temp) value(?,?)")
    updateStream = conn.prepareStatement("update temperatures set temp =? where sensor = ?")
  }

  //调用连接，执行SQL
  override def invoke(value: SensorReading, context: SinkFunction.Context[_]): Unit = {
    //执行更新语句
    updateStream.setDouble(1, value.temperature)
    updateStream.setString(2, value.id)
    updateStream.execute()
    //如果updata没有查到数据，执行插入
    if (updateStream.getUpdateCount == 0) {
      insertStream.setString(1, value.id)
      insertStream.setDouble(2, value.temperature)
      insertStream.execute()
    }
  }

  //关闭时做清理工作
  override def close(): Unit = {
    insertStream.close()
    updateStream.close()
    conn.close()
  }
}
