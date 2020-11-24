package com.rex.demo.study.aboutstate

import com.rex.demo.study.handlelatedata.redirectlatadata.LateReadingsFilter
import com.rex.demo.study.handlelatedata.updateresultbylatadata.UpdatingWindowCountFunction
import com.rex.demo.study.source.{GetExistSource, SensorReading}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

/**
  * descriptions:
  * 关于状态的存储、恢复和清理
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 16:13
  */
object Common {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val readings = GetExistSource.getFromUDSource(env)


    env.execute()
  }


}
