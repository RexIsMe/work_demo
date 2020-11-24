package com.rex.demo.study.processfunction.sideoutput

import com.rex.demo.study.source.{GetExistSource, SensorReading}
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * descriptions:
  * Emitting to Side Outputs(侧输出) 示例
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 14:43
  */
object Common {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings: DataStream[SensorReading] = GetExistSource.getFromUDSource(env)

    val monitoredReadings: DataStream[SensorReading] = readings
      .process(new FreezingMonitor)

    monitoredReadings
      .getSideOutput(new OutputTag[String]("freezing-alarms"))
      .print()

    readings.print()

    env.execute()
  }

}
