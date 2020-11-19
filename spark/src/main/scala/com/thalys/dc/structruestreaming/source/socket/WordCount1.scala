package com.thalys.dc.structruestreaming.source.socket

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


object WordCount1 {

  def main(args: Array[String]): Unit = {
    // 1. 创建 SparkSession. 因为 ss 是基于 spark sql 引擎, 所以需要先创建 SparkSession
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("WordCount1")
      .getOrCreate()
    import spark.implicits._
    // 2. 从数据源(socket)中加载数据.
    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "172.26.55.109")
      .option("port", 9999)
      .load

    // 3. 把每行数据切割成单词
    val words: Dataset[String] = lines.as[String].flatMap(_.split("\\W"))

    // 4. 计算 word count
    val wordCounts: DataFrame = words.groupBy("value").count()

    // 5. 启动查询, 把结果打印到控制台
    val query: StreamingQuery = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start
    query.awaitTermination()

    spark.stop()
  }

}
