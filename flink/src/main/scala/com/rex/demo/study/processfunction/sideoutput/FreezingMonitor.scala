package com.rex.demo.study.processfunction.sideoutput

import com.rex.demo.study.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector

import org.apache.flink.api.scala._

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 15:12
  */
class FreezingMonitor extends ProcessFunction[SensorReading, SensorReading] {
  // define a side output tag
  // 定义一个侧输出标签
  lazy val freezingAlarmOutput: OutputTag[String] =
  new OutputTag[String]("freezing-alarms")

  override def processElement(r: SensorReading,
                              ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                              out: Collector[SensorReading]): Unit = {
    // emit freezing alarm if temperature is below 32F
    if (r.temperature < 32.0) {
      ctx.output(freezingAlarmOutput, s"Freezing Alarm for ${r.id}")
    }
    // forward all readings to the regular output
    out.collect(r)
  }
}
