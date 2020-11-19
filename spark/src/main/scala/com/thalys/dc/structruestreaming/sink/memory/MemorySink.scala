package com.thalys.dc.structruestreaming.sink.memory

import java.util.{Timer, TimerTask}

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object MemorySink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("MemorySink")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "172.26.55.109")
      .option("port", 10000)
      .load

    val words: DataFrame = lines.as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value")
      .count()

    val query: StreamingQuery = words.writeStream
      .outputMode("complete")
      .format("memory") // memory sink
      .queryName("word_count") // 内存临时表名
      .start

    // 测试使用定时器执行查询表
    val timer = new Timer(true)
    val task: TimerTask = new TimerTask {
      override def run(): Unit = spark.sql("select * from word_count").show
    }
    timer.scheduleAtFixedRate(task, 0, 2000)

    query.awaitTermination()

  }

}
