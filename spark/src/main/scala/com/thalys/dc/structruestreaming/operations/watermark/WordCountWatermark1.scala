package com.thalys.dc.structruestreaming.operations.watermark

import java.sql.Timestamp

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
  * watermark 示例
  *
  * 机制总结
  * 1.watermark 在用于基于时间的状态聚合操作时, 该时间可以基于窗口, 也可以基于 event-time本身.
  * 2.输出模式必须是append或update. 在输出模式是complete的时候(必须有聚合), 要求每次输出所有的聚合结果. 我们使用 watermark 的目的是丢弃一些过时聚合数据, 所以complete模式使用watermark无效也无意义.
  * 3.在输出模式是append时, 必须设置 watermark 才能使用聚合操作. 其实, watermark 定义了 append 模式中何时输出聚合聚合结果(状态), 并清理过期状态.
  * 4.在输出模式是update时, watermark 主要用于过滤过期数据并及时清理过期状态.
  * 5.watermark 会在处理当前批次数据时更新, 并且会在处理下一个批次数据时生效使用. 但如果节点发送故障, 则可能延迟若干批次生效.
  * 6.withWatermark 必须使用与聚合操作中的时间戳列是同一列.df.withWatermark("time", "1 min").groupBy("time2").count() 无效
  * 7.withWatermark 必须在聚合之前调用. f.groupBy("time").count().withWatermark("time", "1 min") 无效
  */
object WordCountWatermark1 {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCountWatermark1")
      .getOrCreate()

    import spark.implicits._
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "172.26.55.109")
      .option("port", 9999)
      .load

    // 输入的数据中包含时间戳, 而不是自动添加的时间戳
    val words: DataFrame = lines.as[String].flatMap(line => {
      val split = line.split(",")
      split(1).split(" ").map((_, Timestamp.valueOf(split(0))))
    }).toDF("word", "timestamp")

    import org.apache.spark.sql.functions._

    val wordCounts: Dataset[Row] = words
      // 添加watermark, 参数 1: event-time 所在列的列名 参数 2: 延迟时间的上限.
      .withWatermark("timestamp", "2 minutes")
      .groupBy(window($"timestamp", "10 minutes", "2 minutes"), $"word")
      .count()
//      .orderBy($"window")

    val query: StreamingQuery = wordCounts.writeStream
//      .outputMode("update")
      //在 append 模式中, 仅输出新增的数据, 且输出后的数据无法变更.
      .outputMode("append")
      .trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .option("truncate", "false")
      .start
    query.awaitTermination()
  }

}
