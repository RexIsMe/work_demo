package com.rex.demo.study.processfunction.functionsonwindows

import com.rex.demo.study.source.{GetExistSource, SensorReading}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.{SlidingProcessingTimeWindows, TumblingProcessingTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * descriptions:
  * AggregateFunction 示例 求15秒内各个温度传感器的平均温度
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 14:43
  */
object Common {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings: DataStream[SensorReading] = GetExistSource.getFromUDSource(env)
    val avgTempPerWindow: DataStream[(String, Double)] = readings
      .map(r => (r.id, r.temperature))
      .keyBy(_._1)
      .window(TumblingProcessingTimeWindows.of(Time.seconds(15)))
//      .timeWindow(Timeseconds(15))
      .aggregate(new AvgTempFunction)

    avgTempPerWindow.print()

    env.execute()
  }

}
