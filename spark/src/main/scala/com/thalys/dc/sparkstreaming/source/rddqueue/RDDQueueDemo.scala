package com.thalys.dc.sparkstreaming.source.rddqueue

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 13:29
  */
object RDDQueueDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("RDDQueueDemo").setMaster("local[*]")
    val scc = new StreamingContext(conf, Seconds(5))
    val sc = scc.sparkContext

    // 创建一个可变队列
    val queue: mutable.Queue[RDD[Int]] = mutable.Queue[RDD[Int]]()

    val rddDS: InputDStream[Int] = scc.queueStream(queue, true)
    rddDS.reduce(_ + _).print

    scc.start

    // 循环的方式向队列中添加 RDD
    for (elem <- 1 to 5) {
      queue += sc.parallelize(1 to 100)
      Thread.sleep(2000)
    }

    scc.awaitTermination()
  }
}
