package com.thalys.dc.structruestreaming.sink

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
/**
  *
  */
object FileSink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("Test")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "172.26.55.109")
      .option("port", 9999)
      .load

    val words: DataFrame = lines.as[String].flatMap(line => {
      line.split("\\W+").map(word => {
        (word, word.reverse)
      })
    }).toDF("原单词", "反转单词")

    words.writeStream
      .outputMode("append")
      .format("json") //  // 支持 "orc", "json", "csv"
      .option("path", "/Users/Administrator/Desktop/bigData/data/filesink") // 输出目录
      .option("checkpointLocation", "/Users/Administrator/Desktop/bigData/data/checkpoint/ck1")  // 必须指定 checkpoint 目录
      .start
      .awaitTermination()
  }

}
