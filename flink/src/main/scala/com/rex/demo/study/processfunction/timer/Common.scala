package com.rex.demo.study.processfunction.timer

import com.rex.demo.study.source.{GetExistSource, SensorReading}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

import org.apache.flink.api.scala._
/**
  * descriptions:
  * Process Function 使用示例 （TimerService and Timer）
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 14:43
  */
object Common {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings: DataStream[SensorReading] = GetExistSource.getFromUDSource(env)
    val value: KeyedStream[SensorReading, String] = readings
      // key by sensor id
      .keyBy(_.id)
    value.print()
    // apply ProcessFunction to monitor temperatures
  val value2: DataStream[String] = value.process(new TempIncreaseAlertFunction)
    value2.print()

    env.execute()
  }

}
