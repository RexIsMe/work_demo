//package com.rex.demo.study.aboutstate
//
//import com.rex.demo.study.source.SensorReading
//import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
//import org.apache.flink.configuration.Configuration
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction
//import org.apache.flink.util.Collector
//
///**
//  * descriptions:
//  * 防止状态泄露
//  * 比如一小时内不再产生温度数据的传感器对应的状态数据就可以清理掉了。
//  * author: li zhiqiang
//  * date: 2020 - 11 - 23 17:25
//  */
//class SelfCleaningTemperatureAlertFunction(val threshold: Double)
//  extends KeyedProcessFunction[String, SensorReading, (String, Double, Double)] {
//
//  // the keyed state handle for the last temperature
//  private var lastTempState: ValueState[Double] = _
//  // the keyed state handle for the last registered timer
//  private var lastTimerState: ValueState[Long] = _
//
//  override def open(parameters: Configuration): Unit = {
//    // register state for last temperature
//    val lastTempDesc = new ValueStateDescriptor[Double]("lastTemp", classOf[Double])
//    lastTempState = getRuntimeContext.getState[Double](lastTempDescriptor)
//    // register state for last timer
//    val lastTimerDesc = new ValueStateDescriptor[Long]("lastTimer", classOf[Long])
//    lastTimerState = getRuntimeContext.getState(timestampDescriptor)
//  }
//
//  override def processElement(
//                               reading: SensorReading,
//                               ctx: KeyedProcessFunction
//                                 [String, SensorReading, (String, Double, Double)]#Context,
//                               out: Collector[(String, Double, Double)]): Unit = {
//
//    // compute timestamp of new clean up timer as record timestamp + one hour
//    val newTimer = ctx.timestamp() + (3600 * 1000)
//    // get timestamp of current timer
//    val curTimer = lastTimerState.value()
//    // delete previous timer and register new timer
//    ctx.timerService().deleteEventTimeTimer(curTimer)
//    ctx.timerService().registerEventTimeTimer(newTimer)
//    // update timer timestamp state
//    lastTimerState.update(newTimer)
//
//    // fetch the last temperature from state
//    val lastTemp = lastTempState.value()
//    // check if we need to emit an alert
//    val tempDiff = (reading.temperature - lastTemp).abs
//    if (tempDiff > threshold) {
//      // temperature increased by more than the threshold
//      out.collect((reading.id, reading.temperature, tempDiff))
//    }
//
//    // update lastTemp state
//    this.lastTempState.update(reading.temperature)
//  }
//
//  override def onTimer(
//                        timestamp: Long,
//                        ctx: KeyedProcessFunction
//                          [String, SensorReading, (String, Double, Double)]#OnTimerContext,
//                        out: Collector[(String, Double, Double)]): Unit = {
//
//    // clear all state for the key
//    lastTempState.clear()
//    lastTimerState.clear()
//  }
//}
