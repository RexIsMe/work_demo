package com.rex.demo.study.handlelatedata.updateresultbylatadata

import com.rex.demo.study.source.SensorReading
import org.apache.flink.api.common.state.ValueStateDescriptor
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 16:27
  */
/** A counting WindowProcessFunction that distinguishes between
  * first results and updates. */
class UpdatingWindowCountFunction
  extends ProcessWindowFunction[
    SensorReading, (String, Long, Int, String), String, TimeWindow] {

  override def process(
                        id: String,
                        ctx: Context,
                        elements: Iterable[SensorReading],
                        out: Collector[(String, Long, Int, String)]): Unit = {

    // count the number of readings
    val cnt = elements.count(_ => true)

    // state to check if this is the first evaluation of the window or not
    val isUpdate = ctx.windowState.getState(
      new ValueStateDescriptor[Boolean]("isUpdate", Types.of[Boolean]))

    if (!isUpdate.value()) {
      // first evaluation, emit first result
      out.collect((id, ctx.window.getEnd, cnt, "first"))
      isUpdate.update(true)
    } else {
      // not the first evaluation, emit an update
      out.collect((id, ctx.window.getEnd, cnt, "update"))
    }
  }
}
