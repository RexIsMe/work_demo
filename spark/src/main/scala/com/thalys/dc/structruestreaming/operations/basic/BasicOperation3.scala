package com.thalys.dc.structruestreaming.operations.basic

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 直接执行 sql(重要)
  */
object BasicOperation3 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("BasicOperation")
      .getOrCreate()

    val peopleSchema: StructType = new StructType()
      .add("name", StringType)
      .add("age", LongType)
      .add("sex", StringType)
    val peopleDF: DataFrame = spark.readStream
      .schema(peopleSchema)
      .json("/Users/Administrator/Desktop/bigData/data/filedata")

    peopleDF.createOrReplaceTempView("people") // 创建临时表
    val df: DataFrame = spark.sql("select * from people where age > 20")

    df.writeStream
      .outputMode("append")
      .format("console")
      .start
      .awaitTermination()
  }
}
