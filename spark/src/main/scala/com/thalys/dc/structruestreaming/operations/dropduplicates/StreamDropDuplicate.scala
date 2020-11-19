package com.thalys.dc.structruestreaming.operations.dropduplicates

import java.sql.Timestamp

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
/**
  * 流数据去重示例
  *
  * 注意:
  * 1. dropDuplicates 不可用在聚合之后, 即通过聚合得到的 df/ds 不能调用dropDuplicates
  * 2. 使用watermark - 如果重复记录的到达时间有上限，则可以在事件时间列上定义水印，并使用guid和事件时间列进行重复数据删除。
  *   该查询将使用水印从过去的记录中删除旧的状态数据，这些记录不会再被重复。这限制了查询必须维护的状态量。
  * 3. 没有watermark - 由于重复记录可能到达时没有界限，查询将来自所有过去记录的数据存储为状态。
  *
  * 数据:
  * 1,2019-09-14 11:50:00,dog
  * 2,2019-09-14 11:51:00,dog
  * 1,2019-09-14 11:50:00,dog
  * 3,2019-09-14 11:53:00,dog
  * 1,2019-09-14 11:50:00,dog
  * 4,2019-09-14 11:45:00,dog
  *
  */
object StreamDropDuplicate {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("Test")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "172.26.55.109")
      .option("port", 9999)
      .load()

    val words: DataFrame = lines.as[String].map(line => {
      val arr: Array[String] = line.split(",")
      (arr(0), Timestamp.valueOf(arr(1)), arr(2))
    }).toDF("uid", "ts", "word")

    val wordCounts: Dataset[Row] = words
      .withWatermark("ts", "2 minutes")
      // 去重重复数据 uid 相同就是重复.  可以传递多个列
      .dropDuplicates("uid")

    wordCounts.writeStream
      .outputMode("update")
      .format("console")
      .start
      .awaitTermination()

  }

}
