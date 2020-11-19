package com.thalys.dc.structruestreaming.operations.join

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

/**
  *  双流join 示例
  *
  * 带 watermast 的 inner join
  *
  */
object StreamStream2 {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("StreamStream1")
      .getOrCreate()
    import spark.implicits._

    // 第 1 个 stream
    val nameSexStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "172.26.55.109")
      .option("port", 9999)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1), Timestamp.valueOf(arr(2)))
      }).toDF("name1", "sex", "ts1")
      .withWatermark("ts1", "2 minutes")

    // 第 2 个 stream
    val nameAgeStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "172.26.55.109")
      .option("port", 10000)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1).toInt, Timestamp.valueOf(arr(2)))
      }).toDF("name2", "age", "ts2")
      .withWatermark("ts2", "1 minutes")


    // join 操作
    val joinResult: DataFrame = nameSexStream.join(
      nameAgeStream,
      expr(
        """
          |name1=name2 and
          |ts2 >= ts1 and
          |ts2 <= ts1 + interval 1 minutes
        """.stripMargin)
      //不指定，默认为内连接
//      ,joinType = "left_join"
    )

    joinResult.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(0))
      .start()
      .awaitTermination()
  }

}
