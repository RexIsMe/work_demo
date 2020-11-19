package com.thalys.dc.demo.app

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Date

import com.thalys.dc.demo.entity.AdsInfo
import org.apache.spark.sql._

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 09 15:25
  */
object ReadFromKafkaApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("RealtimeApp")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val dayStringFormatter: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val hmStringFormatter: SimpleDateFormat = new SimpleDateFormat("HH:mm")

    // 1. 从 kafka 读取数据, 为了方便后续处理, 封装数据到 AdsInfo 样例类中
    val adsInfoDS: Dataset[AdsInfo] = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.26.55.116:9092,172.26.55.109:9092,172.26.55.117:9092")
      .option("subscribe", "test")
      .load
      .select("value")
      .as[String]
      .map(v => {
        val split: Array[String] = v.split(",")
        val date: Date = new Date(split(0).toLong)
        AdsInfo(split(0).toLong, new Timestamp(split(0).toLong), dayStringFormatter.format(date), hmStringFormatter.format(date), split(1), split(2), split(3), split(4))
      })
    adsInfoDS.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate", "false")
      .start
      .awaitTermination()
  }

}
