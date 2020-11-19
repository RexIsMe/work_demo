package com.thalys.dc.structruestreaming.sink.kafka

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSink {

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
      .option("port", 10000)
      .load

    val words = lines.as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value")
      .count()
      .map(row => row.getString(0) + "," + row.getLong(1))
      .toDF("value")  // 写入数据时候, 必须有一列 "value"

    words.writeStream
      .outputMode("update")
      .format("kafka")
      .trigger(Trigger.ProcessingTime(0))
      .option("kafka.bootstrap.servers", "172.26.55.116:9092,172.26.55.109:9092,172.26.55.117:9092") // kafka 配置
      .option("topic", "test") // kafka 主题
      .option("checkpointLocation", "/Users/Administrator/Desktop/bigData/data/checkpoint/ck2")  // 必须指定 checkpoint 目录
      .start
      .awaitTermination()
  }

}
