package com.rex.demo.study.handlelatedata.redirectlatadata

import com.rex.demo.study.source.SensorReading
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.OutputTag
import org.apache.flink.util.Collector
/**
  * descriptions:
  * 将迟到元素通过“侧输出”出来
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 16:21
  */
/** A ProcessFunction that filters out late sensor readings and
  * re-directs them to a side output */
class LateReadingsFilter
  extends ProcessFunction[SensorReading, SensorReading] {

  val lateReadingsOut = new OutputTag[SensorReading]("late-readings")

  override def processElement(
                               r: SensorReading,
                               ctx: ProcessFunction[SensorReading, SensorReading]#Context,
                               out: Collector[SensorReading]): Unit = {

    // compare record timestamp with current watermark
    if (r.timestamp < ctx.timerService().currentWatermark()) {
      // this is a late reading => redirect it to the side output
      ctx.output(lateReadingsOut, r)
    } else {
      out.collect(r)
    }
  }
}
