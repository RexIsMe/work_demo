package com.thalys.dc.structruestreaming.source.kafka

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object KafkaSourceDemo2 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaSourceDemo")
      .getOrCreate()
    import spark.implicits._
    // 得到的 df 的 schema 是固定的: key,value,topic,partition,offset,timestamp,timestampType
    val lines: Dataset[String] = spark.readStream
      .format("kafka") // 设置 kafka 数据源
      .option("kafka.bootstrap.servers", "172.26.55.116:9092,172.26.55.109:9092,172.26.55.117:9092")
      .option("subscribe", "test") // 也可以订阅多个主题:   "topic1,topic2"
      .load
      .selectExpr("CAST(value AS String)")
      .as[String]
    val query: DataFrame = lines.flatMap(_.split("\\W+")).groupBy("value").count()
    query.writeStream
      .outputMode("complete")
      .format("console")
      .option("checkpointLocation", "./ck1")  // 下次启动的时候, 可以从上次的位置开始读取
      .start
      .awaitTermination()
  }

}
