package com.rex.demo.study.demo.driver


import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

/**
  * descriptions:
  * flink CEP 示例
  *
  * author: li zhiqiang
  * date: 2020 - 12 - 02 17:11
  */
object ScalaFlinkLoginFail {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime)
    env.setParallelism(1)

    val loginEventStream = env.fromCollection(List(
      LoginEvent("1", "192.168.0.1", "fail", "1558430842"),
      LoginEvent("1", "192.168.0.2", "fail", "1558430843"),
      LoginEvent("1", "192.168.0.3", "fail", "1558430844"),
      LoginEvent("2", "192.168.10.10", "success", "1558430845"),
      LoginEvent("1", "192.168.0.4", "fail", "1558430848"),
      LoginEvent("1", "192.168.0.5", "fail", "1558430868"),
      LoginEvent("1", "192.168.0.6", "fail", "1558430869")
    ))
      .assignAscendingTimestamps(_.eventTime.toLong)

    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.eventType.equals("fail"))
      .next("next")
      .where(_.eventType.equals("fail"))
      .within(Time.milliseconds(10))

    val patternStream: PatternStream[LoginEvent] = CEP.pattern(loginEventStream.keyBy(_.userId), loginFailPattern)

    val loginFailDataStream = patternStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()

        (first.userId + ":" + second.userId, first.ip + ":" + second.ip, first.eventType + ":" + second.eventType)
      })

    loginFailDataStream.printToErr()

    env.execute
  }

}

case class LoginEvent(userId: String, ip: String, eventType: String, eventTime: String)
