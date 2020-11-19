package com.thalys.dc.sparkstreaming.transform.withstate


import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
/**
  * descriptions:
  * updateStateByKey操作允许在使用新信息不断更新状态的同时能够保留他的状态.
  * 需要做两件事情:
  * 1.定义状态. 状态可以是任意数据类型
  * 2.定义状态更新函数. 指定一个函数, 这个函数负责使用以前的状态和新值来更新状态.
  * 在每个阶段, Spark 都会在所有已经存在的 key 上使用状态更新函数, 而不管是否有新的数据在.
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 14:01
  */
object StreamingWordCount2 {
  def main(args: Array[String]): Unit = {
    // 设置将来访问 hdfs 的使用的用户名, 否则会出现权限不够
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val conf = new SparkConf().setAppName("StreamingWordCount2").setMaster("local[*]")
    // 1. 创建SparkStreaming的入口对象: StreamingContext  参数2: 表示事件间隔
    val ssc = new StreamingContext(conf, Seconds(5))
    // 2. 创建一个DStream
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("172.26.55.109", 9999)
    // 3. 一个个的单词
    val words: DStream[String] = lines.flatMap(_.split("""\s+"""))
    // 4. 单词形成元组
    val wordAndOne: DStream[(String, Int)] = words.map((_, 1))


    // 开始
    /*
    1. 定义状态: 每个单词的个数就是我们需要更新的状态
    2. 状态更新函数. 每个key(word)上使用一次更新新函数
        参数1: 在当前阶段 一个新的key对应的value组成的序列  在我们这个案例中是: 1,1,1,1...
        参数2: 上一个阶段 这个key对应的value
    */
    def updateFunction(newValue: Seq[Int], runningCount: Option[Int]): Option[Int] = {
      // 新的总数和状态进行求和操作
      val newCount: Int = (0 /: newValue) (_ + _) + runningCount.getOrElse(0)
      Some(newCount)
    }
    // 设置检查点: 使用updateStateByKey必须设置检查点
    ssc.sparkContext.setCheckpointDir("hdfs://hadoop-master:9000/spark/checkpoint")
    val stateDS: DStream[(String, Int)] = wordAndOne.updateStateByKey[Int](updateFunction _)
    //结束

    //6. 显示
    stateDS.print
    //7. 启动流失任务开始计算
    ssc.start()
    //8. 等待计算结束才推出主程序
    ssc.awaitTermination()
    ssc.stop(false)
  }
}
