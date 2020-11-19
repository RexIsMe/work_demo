package com.thalys.dc.structruestreaming.sink.foreach

import java.util.Properties

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}
/**
  * ForeachBatch Sink 是 spark 2.4 才新增的功能, 该功能只能用于输出批处理的数据.
  * 将统计结果同时输出到本地文件和 mysql 中
  */
object ForeachBatchSink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("ForeachBatchSink")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "172.26.55.109")
      .option("port", 10000)
      .load

    val wordCount: DataFrame = lines.as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value")
      .count()

    val props = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "t4*9&/y?c,h.e17!")
    val query: StreamingQuery = wordCount.writeStream
      .outputMode("complete")
      .foreachBatch((df, batchId) => {  // 当前分区id, 当前批次id
        if (df.count() != 0) {
          df.cache()
          df.write.json(s"./$batchId")
          df.write.mode("overwrite").jdbc("jdbc:mysql://172.26.55.109:3306/dc", "word_count", props)
        }
      })
      .start()


    query.awaitTermination()

  }

}
