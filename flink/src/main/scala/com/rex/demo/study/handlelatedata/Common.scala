package com.rex.demo.study.handlelatedata

import com.rex.demo.study.handlelatedata.redirectlatadata.LateReadingsFilter
import com.rex.demo.study.handlelatedata.updateresultbylatadata.UpdatingWindowCountFunction
import com.rex.demo.study.source.{GetExistSource, SensorReading, SensorSource}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
  * descriptions:
  * 迟到数据的处理
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

//    throwLateData(readings)
//    redirectLateData(readings)
    updateResultByLateData(readings)

    env.execute()
  }

  /**
    * 默认丢弃迟到元素
    * @param readings
    */
  def throwLateData(readings: DataStream[SensorReading]): Unit ={

  }

  /**
    * 重定向迟到元素
    * @param readings
    */
  def redirectLateData(readings: DataStream[SensorReading]): Unit ={
    val filteredReadings: DataStream[SensorReading] = readings
      .process(new LateReadingsFilter)

    // retrieve late readings
    val lateReadings: DataStream[SensorReading] = filteredReadings
      .getSideOutput(new OutputTag[SensorReading]("late-readings"))

    lateReadings.print()
  }

  /**
    * 使用迟到元素更新窗口计算结果(Updating Results by Including Late Events)
    * @param readings
    */
  def updateResultByLateData(readings: DataStream[SensorReading]): Unit ={
    val countPer10Secs: DataStream[(String, Long, Int, String)] = readings
      .keyBy(_.id)
      .timeWindow(Time.seconds(10))
      // process late readings for 5 additional seconds
      .allowedLateness(Time.seconds(5))
      // count readings and update results if late readings arrive
      .process(new UpdatingWindowCountFunction)

    countPer10Secs.print()
  }

}
