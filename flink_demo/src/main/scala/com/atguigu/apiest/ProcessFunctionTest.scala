package com.atguigu.apiest

import com.atguigu.apiest.SourceTest.SensorReading
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(60000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(1000000)
    env.getCheckpointConfig.setFailOnCheckpointingErrors(false) //checkpoint发生错误时是否关闭整个job
    //    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)//同时允许几个checkpoint同时进行
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000) //2个checkpoint时间间隔
    env.getCheckpointConfig.enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION) //在取消工作时删除外部化的检查点。
    env.setRestartStrategy(RestartStrategies.failureRateRestart(3, org.apache.flink.api.common.time.Time.seconds(300),
      org.apache.flink.api.common.time.Time.seconds(10))) //重启策略

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
    val processedStream = dataStream.keyBy(_.id)
      .process(new TempIncreAlert())

    val processedStream2 = dataStream.keyBy(_.id)
      //      .process(new TempChangeAlert(10.0))
      .flatMap(new TempChangeAlert2(10.0))

    val processedStream3 = dataStream.keyBy(_.id)
      .flatMapWithState[(String, Double, Double), Double] {
      //如果没有状态的话，也就没有数据来过，那么就将当前数据温度值存入状态
      case (input: SensorReading, None) => (List.empty, Some(input.temperature))
      //如果有状态的话，就应该与上一次的温度比较差值，如果大于阈值就报警
      case (input: SensorReading, lastTemp: Some[Double]) =>
        val diff = (input.temperature - lastTemp.get).abs
        if (diff > 10.0) {
          (List((input.id, lastTemp.get, input.temperature)), Some(input.temperature))
        } else {
          (List.empty, Some(input.temperature))
        }
    }

    processedStream.print("process")
    processedStream2.print("process2")
    processedStream3.print("process3")
    env.execute("process function test")
  }
}

class TempIncreAlert() extends KeyedProcessFunction[String, SensorReading, String] {
  //定义一个状态，用来保存上一个数据的温度值
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  //定义一个状态，用来保存定时器的时间戳
  lazy val currentTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("currentTimer", classOf[Long]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, String]#Context, collector: Collector[String]): Unit = {
    //先取出上一个温度值
    val preTemp = lastTemp.value()
    //更新温度值
    lastTemp.update(i.temperature)
    val curTimerTs = currentTimer.value()
    //温度上升切没有设置过定时器,则注册定时器
    if (i.temperature > preTemp && curTimerTs == 0) {
      //获取当前处理时间
      val timerTs = context.timerService().currentProcessingTime() + 10000L
      //注册定时器
      context.timerService().registerProcessingTimeTimer(timerTs)
      currentTimer.update(timerTs)
    } else if (i.temperature < preTemp || preTemp == 0.0) {
      //如果温度下降，或是第一条数据
      //删除定时器
      context.timerService().deleteProcessingTimeTimer(curTimerTs)
      //清空状态
      currentTimer.clear()
    }
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
    //输出报警信息
    out.collect(ctx.getCurrentKey + "连续上升")
    currentTimer.clear()
  }
}

class TempChangeAlert(str: Double) extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
  //定义一个状态变量，保存上一次的温度值
  lazy val lastTempState: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))

  override def processElement(i: SensorReading, context: KeyedProcessFunction[String, SensorReading, (String, Double, Double)]#Context, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp = lastTempState.value()
    //用当前的温度值和上一次的温度值计算差值，如果大于阈值，输出报警信息
    val diff = (i.temperature - lastTemp).abs
    if (diff > str) {
      collector.collect((i.id, lastTemp, i.temperature))
    }
    lastTempState.update(i.temperature)
  }
}

class TempChangeAlert2(d: Double) extends RichFlatMapFunction[SensorReading, (String, Double, Double)] {
  private var lastTempState: ValueState[Double] = _

  override def open(parameters: Configuration): Unit = {
    //初始化的时候声明state变量
    lastTempState = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double]))
  }

  override def flatMap(in: SensorReading, collector: Collector[(String, Double, Double)]): Unit = {
    //获取上次的温度值
    val lastTemp = lastTempState.value()
    //用当前的温度值和上一次的温度值计算差值，如果大于阈值，输出报警信息
    val diff = (in.temperature - lastTemp).abs
    if (diff > d) {
      collector.collect((in.id, lastTemp, in.temperature))
    }
    lastTempState.update(in.temperature)
  }
}
