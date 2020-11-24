package com.rex.demo.study.transformations

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 11:09
  */
object Reduce {

  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)


    val unit: DataStream[(String, List[String])] = env.fromElements(("en", List("tea")), ("fr", List("vin")), ("en", List("cake")))
    val unit2: KeyedStream[(String, List[String]), String] = unit.keyBy(new KeySelector[(String, List[String]), String] {
      override def getKey(in: (String, List[String])): String = in._1
    })
    unit2.reduce((x, y) => (x._1, x._2 ::: y._2)).print()

//    val unit3: KeyedStream[(String, List[String]), Tuple] = unit.keyBy(0)
//    unit3.reduce((x, y) => (x._1, x._2 ::: y._2)).print()

    env.execute()
  }

}
