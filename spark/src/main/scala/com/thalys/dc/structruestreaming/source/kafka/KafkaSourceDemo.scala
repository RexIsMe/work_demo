package com.thalys.dc.structruestreaming.source.kafka

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSourceDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("KafkaSourceDemo")
      .getOrCreate()

    // 得到的 df 的 schema 是固定的: key,value,topic,partition,offset,timestamp,timestampType
    val df: DataFrame = spark.readStream
      .format("kafka") // 设置 kafka 数据源
      .option("kafka.bootstrap.servers", "172.26.55.116:9092,172.26.55.109:9092,172.26.55.117:9092")
      .option("subscribe", "test") // 也可以订阅多个主题:   "topic1,topic2"
      .load


    df.writeStream
      .outputMode("update")
      .format("console")
      .trigger(Trigger.Continuous(1000))
      .start
      .awaitTermination()


  }

}
