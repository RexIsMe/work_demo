package com.thalys.dc.structruestreaming.sink.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 以 batch 方式输出数据
  * 这种方式输出离线处理的结果, 将已存在的数据分为若干批次进行处理. 处理完毕后程序退出.
  */
object KafkaSink2 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[1]")
      .appName("Test")
      .getOrCreate()
    import spark.implicits._

    val wordCount: DataFrame = spark.sparkContext.parallelize(Array("hello hello thalys", "thalys, hello"))
      .toDF("word")
      .groupBy("word")
      .count()
      .map(row => row.getString(0) + "," + row.getLong(1))
      .toDF("value")  // 写入数据时候, 必须有一列 "value"

    wordCount.write  // batch 方式
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.26.55.116:9092,172.26.55.109:9092,172.26.55.117:9092") // kafka 配置
      .option("topic", "test") // kafka 主题
      .save()
  }

}
