package com.thalys.dc.structruestreaming.operations.basic

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * 弱类型 api(了解)
  */
object BasicOperation {

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

    val df: DataFrame = peopleDF.select("age","name", "sex")
      .where("age > 20") // 弱类型 api

    df.writeStream
      .outputMode("append")
      .format("console")
      .start
      .awaitTermination()
  }

}
