package com.thalys.dc.structruestreaming.source.rate

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object RateSourceDemo {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("RateSourceDemo")
      .getOrCreate()

    val rows: DataFrame = spark.readStream
      .format("rate") // 设置数据源为 rate
      .option("rowsPerSecond", 10) // 设置每秒产生的数据的条数, 默认是 1
      .option("rampUpTime", 1) // 设置多少秒到达指定速率 默认为 0
      .option("numPartitions", 2) /// 设置分区数  默认是 spark 的默认并行度
      .load

    rows.writeStream
      .outputMode("append")
      .trigger(Trigger.Continuous(1000))
      .format("console")
      .start()
      .awaitTermination()
  }

}
