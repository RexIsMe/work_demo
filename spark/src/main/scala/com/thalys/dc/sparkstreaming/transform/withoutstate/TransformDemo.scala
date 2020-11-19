package com.thalys.dc.sparkstreaming.transform.withoutstate

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * descriptions:
  * transform 原语允许 DStream上执行任意的RDD-to-RDD函数。
  * 可以用来执行一些 RDD 操作, 即使这些操作并没有在 SparkStreaming 中暴露出来.
  * 该函数每一批次调度一次。其实也就是对DStream中的RDD应用转换。
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 13:57
  */
object TransformDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Practice").setMaster("local[2]")

    val sctx = new StreamingContext(conf, Seconds(3))
    val dstream: ReceiverInputDStream[String] = sctx.socketTextStream("172.26.55.109", 9999)

    //使用transform操作DS中的rdd
    val resultDStream = dstream.transform(rdd => {
      rdd.flatMap(_.split("\\W")).map((_, 1)).reduceByKey(_ + _)
    })
    resultDStream.print

    //直接操作DS
    val value: DStream[(String, Int)] = dstream.flatMap(_.split("\\W")).map((_, 1)).reduceByKey(_ + _)
    value.print()


    sctx.start
    sctx.awaitTermination()
  }
}
