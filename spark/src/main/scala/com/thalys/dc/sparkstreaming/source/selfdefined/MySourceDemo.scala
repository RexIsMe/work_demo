package com.thalys.dc.sparkstreaming.source.selfdefined

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * descriptions:
  * 测试使用自定义数据源
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 13:45
  */
object MySourceDemo {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("StreamingWordCount").setMaster("local[*]")
    // 1. 创建SparkStreaming的入口对象: StreamingContext  参数2: 表示事件间隔
    val ssc = new StreamingContext(conf, Seconds(5))
    // 2. 创建一个DStream
    val lines: ReceiverInputDStream[String] = ssc.receiverStream[String](MySource("172.26.55.109", 9999))
    // 3. 一个个的单词
    val words: DStream[String] = lines.flatMap(_.split("""\s+"""))
    // 4. 单词形成元组
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
    // 5. 统计单词的个数
    val count: DStream[(String, Int)] = wordAndOne.reduceByKey(_ + _)
    //6. 显示
    count.print
    //7. 启动流式任务开始计算
    ssc.start()
    //8. 等待计算结束才退出主程序
    ssc.awaitTermination()
    ssc.stop(false)
  }
}
