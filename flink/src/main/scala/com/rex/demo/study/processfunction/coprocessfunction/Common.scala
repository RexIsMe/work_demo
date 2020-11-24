package com.rex.demo.study.processfunction.coprocessfunction

import com.rex.demo.study.source.{GetExistSource, SensorReading}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}

/**
  * descriptions:
  * CoProcessFunction 示例
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 14:43
  */
object Common {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val readings: DataStream[SensorReading] = GetExistSource.getFromUDSource(env)

    // filter switches enable forwarding of readings
    val filterSwitches: DataStream[(String, Long)] = env
      .fromCollection(Seq(
        ("sensor_2", 10 * 1000L),
        ("sensor_7", 60 * 1000L)
      ))

    val forwardedReadings = readings
      // connect readings and switches
      .connect(filterSwitches)
      // key by sensor ids
      .keyBy(_.id, _._1)
      // apply filtering CoProcessFunction
      .process(new ReadingFilter)

    forwardedReadings.print()

    env.execute()
  }

}
