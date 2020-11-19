package com.thalys.dc.scbi.structruestreamingimpl.app

import com.thalys.dc.scbi.structruestreamingimpl.entity.DCRecordEntity
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import spark.implicits._
import scala.collection.mutable

/**
  * descriptions:
  * scbi 功能迁移实现
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 09 17:15
  */
object SCBIApp {

  def main(args: Array[String]): Unit = {
    version2
  }

  /**
    * 版本一
    *
    * 消费kafka指定topic数据
    *
    * @param args
    */
  def version2: Unit = {
//    val spark: SparkSession = SparkSession
//      .builder()
//      .appName("RealtimeApp")
//      .master("local[*]")
//      .getOrCreate()
    spark.sparkContext.setLogLevel("warn")
    // 从 kafka 读取数据, 为了方便后续处理, 封装数据到 AdsInfo 样例类中
    val schema = getSchema(spark)
    val frame: DataFrame = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "172.26.55.116:9092,172.26.55.109:9092,172.26.55.117:9092")
      .option("subscribe", "test")
//      .option("includeTimestamp", true)
      .load
      .selectExpr("CAST(value AS String) as json")
      .select(from_json($"json", schema=schema).as("data"))
      .select("data.*")
//      .selectExpr("CAST(timestamp AS Timestamp)")
//      .withWatermark("timestamp", "24 hours") // 都是统计每天的数据, 对迟到24小时的数据废弃不用

    // 打印数据，查看是否正常接收数据
//    frame.writeStream
//      .outputMode("append")
//      .format("console")
//      .start
//      .awaitTermination()


    val dcRecordDS: Dataset[DCRecordEntity] = frame.as[DCRecordEntity]
    GetBatchNoApp.statBlackList(dcRecordDS)

  }

  /**
    *
    * @param spark
    */
  def getSchema(spark: SparkSession): StructType ={
    import spark.implicits._
    val stock = StructType(mutable.Seq(
      StructField("accountingMl", DataTypes.StringType),
      StructField("manufacturerName", DataTypes.StringType),
      StructField("customerEnableCode", DataTypes.StringType),
      StructField("ownerId", DataTypes.IntegerType),
      StructField("warehouseCategoryCode", DataTypes.StringType),
      StructField("organizationId", DataTypes.LongType),
      StructField("sterilizationBatchNo", DataTypes.StringType),
      StructField("inventoryNumber", DataTypes.IntegerType),
      StructField("effectiveDays", DataTypes.IntegerType),
      StructField("packageUnitName", DataTypes.StringType),
      StructField("warehouseAreaDesc", DataTypes.StringType),
      StructField("commodityCode", DataTypes.StringType),
      StructField("registerNumber", DataTypes.StringType),
      StructField("warehouseCategoryId", DataTypes.IntegerType),
      StructField("batchNo", DataTypes.StringType),
      StructField("brandName", DataTypes.StringType),
      StructField("commoditySpec", DataTypes.StringType),
      StructField("commodityNumber", DataTypes.StringType),
      StructField("organizationName", DataTypes.StringType),
      StructField("supplierEnableCode", DataTypes.StringType),
      StructField("logicAreaId", DataTypes.IntegerType),
      StructField("commodityType", DataTypes.StringType),
      StructField("productionPlace", DataTypes.StringType),
      StructField("professionalGroup", DataTypes.StringType),
      StructField("customerName", DataTypes.StringType),
      StructField("quotiety", DataTypes.IntegerType),
      StructField("warehouseId", DataTypes.IntegerType),
      StructField("goodsLocationId", DataTypes.IntegerType),
      StructField("storageCondition", DataTypes.StringType),
      StructField("inventoryOrganizationCode", DataTypes.StringType),
      StructField("productionLicense", DataTypes.StringType),
      StructField("producedDate", DataTypes.LongType),
      StructField("supplierId", DataTypes.IntegerType),
      StructField("warehouseAreaCode", DataTypes.StringType),
      StructField("accountingName", DataTypes.StringType),
      StructField("ownerCode", DataTypes.StringType),
      StructField("allocatedQuantity", DataTypes.IntegerType),
      StructField("packageUnitId", DataTypes.IntegerType),
      StructField("customerCode", DataTypes.StringType),
      StructField("supplierCode", DataTypes.StringType),
      StructField("warehouseName", DataTypes.StringType),
      StructField("warehouseCode", DataTypes.StringType),
      StructField("inventoryOrganizationName", DataTypes.StringType),
      StructField("goodsLocation", DataTypes.StringType),
      StructField("ownerName", DataTypes.StringType),
      StructField("isSimplelevy", DataTypes.StringType),
      StructField("logicAreaName", DataTypes.StringType),
      StructField("supplierName", DataTypes.StringType),
      StructField("quantity", DataTypes.IntegerType),
      StructField("inventoryOrganizationId", DataTypes.StringType),
      StructField("deviceClassifyType", DataTypes.StringType),
      StructField("warehouseCategoryName", DataTypes.StringType),
      StructField("commodityId", DataTypes.IntegerType),
      StructField("commodityRemark", DataTypes.StringType),
      StructField("deviceClassify", DataTypes.StringType),
      StructField("warehouseAreaId", DataTypes.IntegerType),
      StructField("logicAreaCode", DataTypes.StringType),
      StructField("coldChainMark", DataTypes.StringType),
      StructField("accountingOne", DataTypes.StringType),
      StructField("classifyIdLevel1", DataTypes.StringType),
      StructField("classifyIdLevel2", DataTypes.StringType),
      StructField("registerName", DataTypes.StringType),
      StructField("effectiveDate", DataTypes.LongType),
      StructField("commodityName", DataTypes.StringType)
    ))
    StructType(mutable.Seq(
      StructField("ip", DataTypes.StringType),
      StructField("timestamp", DataTypes.LongType),
      StructField("companyName", DataTypes.StringType),
      StructField("businessName", DataTypes.StringType),
      StructField("dataList", DataTypes.createArrayType(stock))
    ))
  }

}
