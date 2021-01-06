package com.thalys.dc.sparkcore.demo.app

import java.text.SimpleDateFormat
import java.util.Date

import com.alibaba.fastjson.{JSON, JSONObject}
import com.thalys.dc.scbi.structruestreamingimpl.entity.StockEntity
import com.thalys.dc.sparkcore.demo.entity.DCRecordEntity
import org.apache.commons.lang3.time.DateUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * descriptions:
  * ods到ads数据处理示例
  * 使用fastjson做json字符串与对象之间的转换
  *
  * author: li zhiqiang
  * date: 2020 - 12 - 21 13:28
  */
object OdsToAdsDemo3 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("WriteTableToHive")
//      .config("spark.sql.warehouse.dir","D:\\reference-data\\spark01\\spark-warehouse")
      .enableHiveSupport()
      .getOrCreate()
//    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("test")
//    val sc = new SparkContext(sparkConf)
//    val sqlContext = new HiveContext(sc)

    import spark.implicits._
    // TODO 3. 读取文件:
    //  3.1)从classpath中获取
    // Thread.currentThread().getContextClassLoader().getResourceAsStream()
    //  3.2) 从项目的环境中获取
    val frame: DataFrame = spark.read.json("hdfs://hadoop-master:9000/origin_data/dc/log/2020-12-21")

    write2HdfsViaHive(spark, frame)
//    write2HdfsViaDF(frame)
    // TODO 9. 释放连接
    spark.stop()

  }

  def stockEntityReduceFunction(se1:StockEntity, se2:StockEntity):StockEntity = {
    if(se1.equals(se2)){
      val sum: Int = se1.inventoryNumber + se2.inventoryNumber
      println(se1.inventoryNumber + ":" + se2.inventoryNumber)
      se1.inventoryNumber = sum
    }
    se1
  }


  def write2HdfsViaHive(sqlContext: SparkSession, df:DataFrame) = {

    /*
    1. 建表语句
    create external table testlog(sourceip string, port string, url string,
    time string) partitioned by ( dayid string, hourid string)
    stored as orc location '/tmp/sparkhive2';
    2. 开启动态分区
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    */

    val tmpLogTable = "tmpLog"
    df.registerTempTable(tmpLogTable)

    sqlContext.sql("use test2")
    sqlContext.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val value: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
    val today: String = value.format(new Date)

    val insertSQL =
      s"""
         |insert into ods_stock partition(dt = $today)
         |select ip as message
         |from $tmpLogTable
      """.stripMargin
    sqlContext.sql(insertSQL)

  }

  def write2HdfsViaDF(df:DataFrame) = {
    // df.show(false)
    // df.printSchema()
    val outputPath = "/spark/sparkdf"
    df.write.format("orc").partitionBy("dayid", "hourid").mode(SaveMode.Overwrite).
      save(outputPath)
  }



}
