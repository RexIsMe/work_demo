package com.thalys.dc.structruestreaming.operations.join

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 流与静态数据join 示例
  *
  * 模拟的静态数据:
  * lisi,male
  * zhiling,female
  * zs,male
  * 模拟的流式数据:
  * lisi,20
  * zhiling,40
  * ww,30
  *
  *
  */
object StreamingStatic {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("StreamingStatic")
      .getOrCreate()
    import spark.implicits._

    // 1. 静态 df
    val arr = Array(("lisi", "male"), ("zhiling", "female"), ("zs", "male"));
    var staticDF: DataFrame = spark.sparkContext.parallelize(arr).toDF("name", "sex")

    // 2. 流式 df
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "172.26.55.109")
      .option("port", 9999)
      .load()
    val streamDF: DataFrame = lines.as[String].map(line => {
      val arr = line.split(",")
      (arr(0), arr(1).toInt)
    }).toDF("name", "age")

    // 3. join   等值内连接  a.name=b.name
//    val joinResult: DataFrame = streamDF.join(staticDF, "name")
    //外连接
    val joinResult: DataFrame = streamDF.join(staticDF, Seq("name"), "left")

    // 4. 输出
    joinResult.writeStream
      .outputMode("append")
      .format("console")
      .start
      .awaitTermination()
  }

}
