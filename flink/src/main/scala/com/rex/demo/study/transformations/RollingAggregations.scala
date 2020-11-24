package com.rex.demo.study.transformations

import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._

/**
  * descriptions:
  * 滚动聚合算子示例
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 10:42
  */
object RollingAggregations {

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置流的时间为Processing Time
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // 设置并行度为1，如果不设置，那么默认为当前机器的cpu的数量
    env.setParallelism(1)


    val inputStream: DataStream[(Int, Int, Int)] = env.fromElements(
      (1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

//    val resultStream: DataStream[(Int, Int, Int)] = inputStream
//      .keyBy(0) // key on first field of the tuple
//      .sum(1)   // sum the second field of the tuple in place
//    resultStream.print()


    println("=============我是分割线============")

    /**
      * 使用KeySelector方式
      */
    val value: KeyedStream[(Int, Int, Int), Int] = inputStream
      .keyBy(new KeySelectTest)

    //流发生了类型转换
    val value2: DataStream[(Int, Int, Int)] = value
      .sum(1)

    value2.print()


    env.execute()
  }


  class KeySelectTest extends KeySelector[(Int, Int, Int), Int]{
    override def getKey(in: (Int, Int, Int)): Int = in._1
  }

}
