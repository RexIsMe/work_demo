package com.thalys.dc.sparkstreaming.demo

import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 13:12
  */
object StreamingWordCount {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    // 1. 创建SparkStreaming的入口对象: StreamingContext  参数2: 表示事件间隔   内部会创建 SparkContext
    val ssc = new StreamingContext(conf, Seconds(3))
    // 2. 创建一个DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("172.26.55.109", 9999)
    // 3. 一个个的单词
    val words: DStream[String] = lines.flatMap(_.split("""\s+"""))
    // 4. 单词形成元组
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    // 5. 统计单词的个数
    val count: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //6. 显示
    println("aaa")
    count.print
    //7. 开始接受数据并计算
    ssc.start()
    //8. 等待计算结束(要么手动退出,要么出现异常)才退出主程序
    ssc.awaitTermination()
  }

}
