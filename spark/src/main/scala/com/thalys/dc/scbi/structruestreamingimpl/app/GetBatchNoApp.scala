package com.thalys.dc.scbi.structruestreamingimpl.app



import java.sql.{Connection, DriverManager, PreparedStatement, Timestamp}

import com.thalys.dc.scbi.structruestreamingimpl.entity.{DCRecordEntity, StockWildEntity}
import com.thalys.dc.scbi.structruestreamingimpl.util.Md5
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql._
import spark.implicits._

/**
  * descriptions:
  * scbi-getBatchNo 需求实现
  *
  * 聚合当前数据
  * 将当前聚合结果和当天之前的历史结果相加后保存到mysql
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 10 9:47
  */
object GetBatchNoApp {

  def statBlackList(dcRecordDS: Dataset[DCRecordEntity]): Unit = {
    //先将流解析成明细表
    val stockWildDS: Dataset[StockWildEntity] = dcRecordDS
      .flatMap(row => {
        val list = row.dataList.toList
        val entities: List[StockWildEntity] = list.map(v => StockWildEntity(row.ip, row.timestamp, row.companyName, row.businessName, v.accountingMl,v.manufacturerName,v.customerEnableCode,v.ownerId,v.warehouseCategoryCode,v.organizationId,v.sterilizationBatchNo,v.inventoryNumber,v.effectiveDays, v.packageUnitName: String,v.warehouseAreaDesc, v.commodityCode, v.registerNumber, v.warehouseCategoryId, v.batchNo, v.brandName, v.commoditySpec, v.commodityNumber, v.organizationName, v.supplierEnableCode, v.logicAreaId, v.commodityType, v.productionPlace, v.professionalGroup, v.customerName, v.quotiety, v.warehouseId, v.goodsLocationId, v.storageCondition, v.inventoryOrganizationCode, v.productionLicense, v.producedDate, v.supplierId, v.warehouseAreaCode, v.accountingName, v.ownerCode, v.allocatedQuantity, v.packageUnitId, v.customerCode, v.supplierCode, v.warehouseName, v.warehouseCode, v.inventoryOrganizationName, v.goodsLocation, v.ownerName, v.isSimplelevy, v.logicAreaName, v.supplierName, v.quantity, v.inventoryOrganizationId, v.deviceClassifyType, v.warehouseCategoryName, v.commodityId, v.commodityRemark, v.deviceClassify, v.warehouseAreaId, v.logicAreaCode, v.coldChainMark, v.accountingOne, v.classifyIdLevel1, v.classifyIdLevel2, v.registerName, v.effectiveDate, v.commodityName))
        entities
      })

    //按需求聚合当前数据
    val frame: DataFrame = stockWildDS.toDF()
    frame.createOrReplaceTempView("stock_wild") // 创建临时表
    val result: DataFrame = spark.sql(
      """
        |select
        | ownerName ,
        |	goodsLocation ,
        |	inventoryOrganizationId ,
        |	registerNumber ,
        |	batchNo ,
        |	sterilizationBatchNo ,
        |	effectiveDate ,
        |	producedDate ,
        |	warehouseName ,
        |	supplierName ,
        |	manufacturerName ,
        |	commodityName ,
        |	commoditySpec ,
        |	classifyIdLevel1 ,
        |	classifyIdLevel2 ,
        |	brandName ,
        |	registerName ,
        |	logicAreaName ,
        |	packageUnitName ,
        |	commodityCode ,
        |	customerName ,
        |	commodityNumber,
        | sum(inventoryNumber) sumInventoryNumber
        |from stock_wild
        |group by
        | ownerName ,
        |	goodsLocation ,
        |	inventoryOrganizationId ,
        |	registerNumber ,
        |	batchNo ,
        |	sterilizationBatchNo ,
        |	effectiveDate ,
        |	producedDate ,
        |	warehouseName ,
        |	supplierName ,
        |	manufacturerName ,
        |	commodityName ,
        |	commoditySpec ,
        |	classifyIdLevel1 ,
        |	classifyIdLevel2 ,
        |	brandName ,
        |	registerName ,
        |	logicAreaName ,
        |	packageUnitName ,
        |	commodityCode ,
        |	customerName ,
        |	commodityNumber
      """.stripMargin)

    //将聚合结果更新到mysql中的历史数据
    result
      .repartition(4)
      .writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("3 seconds"))
      .foreach(new ForeachWriter[Row] {
        var conn: Connection = _
        var ps: PreparedStatement = _
        var batchCount = 0

        // 一般用于 打开“连接”. 返回 false 表示跳过该分区的数据
        override def open(partitionId: Long, epochId: Long): Boolean = {
          println("open ..." + partitionId + "  " + epochId)
          Class.forName("com.mysql.jdbc.Driver")
          conn = DriverManager.getConnection("jdbc:mysql://172.26.55.109:3306/dc?useUnicode=true&characterEncoding=utf8", "root", "t4*9&/y?c,h.e17!")
          // 插入数据, 当有重复的 key 的时候更新
          val sql ="""
                     |insert into ads_stock_realtime(
                     |md5_key,owner_name,goods_location,inventory_organization_id,register_number,batch_no,sterilization_batch_no,effective_date,produced_date,warehouse_name,supplier_name,manufacturer_name,commodity_name,commodity_spec,classify_id_level1,classify_id_level2,brand_name,register_name,logic_area_name,package_unit_name,commodity_code,customer_name,commodity_number,sum_inventory_number,sum_inventory_number_realtime)
                     |values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                     |on duplicate key
                     |update md5_key=?, owner_name =  ?, goods_location =  ?, inventory_organization_id =  ?, register_number =  ?, batch_no =  ?, sterilization_batch_no =  ?, effective_date =  ?, produced_date =  ?, warehouse_name =  ?, supplier_name =  ?, manufacturer_name =  ?, commodity_name =  ?, commodity_spec =  ?, classify_id_level1 =  ?, classify_id_level2 =  ?, brand_name =  ?, register_name =  ?, logic_area_name =  ?, package_unit_name =  ?, commodity_code =  ?, customer_name =  ?, commodity_number =  ?, sum_inventory_number =  sum_inventory_number,
                     | sum_inventory_number_realtime = sum_inventory_number + ?
                   """.stripMargin

          ps = conn.prepareStatement(sql)

          conn != null && !conn.isClosed && ps != null
        }

        // 把数据写入到连接
        override def process(value: Row): Unit = {
          println("process ...." + value)
          var ownerName: String = value.getString(0)
          var goodsLocation: String = value.getString(1)
          var inventoryOrganizationId: String = value.getString(2)
          var registerNumber: String = value.getString(3)
          var batchNo: String = value.getString(4)
          var sterilizationBatchNo: String = value.getString(5)
          var effectiveDate: Long = value.getLong(6)
          var producedDate: Long = value.getLong(7)
          var warehouseName: String = value.getString(8)
          var supplierName: String = value.getString(9)
          var manufacturerName: String = value.getString(10)
          var commodityName: String = value.getString(11)
          var commoditySpec: String = value.getString(12)
          var classifyIdLevel1: String = value.getString(13)
          var classifyIdLevel2: String = value.getString(14)
          var brandName: String = value.getString(15)
          var registerName: String = value.getString(16)
          var logicAreaName: String = value.getString(17)
          var packageUnitName: String = value.getString(18)
          var commodityCode: String = value.getString(19)
          var customerName: String = value.getString(20)
          var commodityNumber: String = value.getString(21)
          var sumInventoryNumber: Long = value.getLong(22)
          var sumInventoryNumberRealtime: Long = sumInventoryNumber

          val md5Key: String = Md5.hashMD5(ownerName.toString + goodsLocation.toString + inventoryOrganizationId.toString + registerNumber.toString + batchNo.toString + sterilizationBatchNo.toString + effectiveDate.toString + producedDate.toString + warehouseName.toString + supplierName.toString + manufacturerName.toString + commodityName.toString + commoditySpec.toString + classifyIdLevel1.toString + classifyIdLevel2.toString + brandName.toString + registerName.toString + logicAreaName.toString + packageUnitName.toString + commodityCode.toString + customerName.toString + commodityNumber)

          ps.setString(1, md5Key)
          ps.setString(2, ownerName)
          ps.setString(3, goodsLocation)
          ps.setString(4, inventoryOrganizationId)
          ps.setString(5, registerNumber)
          ps.setString(6, batchNo)
          ps.setString(7, sterilizationBatchNo)
          ps.setLong(8, effectiveDate)
          ps.setLong(9, producedDate)
          ps.setString(10, warehouseName)
          ps.setString(11, supplierName)
          ps.setString(12, manufacturerName)
          ps.setString(13, commodityName)
          ps.setString(14, commoditySpec)
          ps.setString(15, classifyIdLevel1)
          ps.setString(16, classifyIdLevel2)
          ps.setString(17, brandName)
          ps.setString(18, registerName)
          ps.setString(19, logicAreaName)
          ps.setString(20, packageUnitName)
          ps.setString(21, commodityCode)
          ps.setString(22, customerName)
          ps.setString(23, commodityNumber)
          ps.setLong(24, sumInventoryNumber)
          ps.setLong(25, sumInventoryNumberRealtime)

          ps.setString(26, md5Key)
          ps.setString(27, ownerName)
          ps.setString(28, goodsLocation)
          ps.setString(29, inventoryOrganizationId)
          ps.setString(30, registerNumber)
          ps.setString(31, batchNo)
          ps.setString(32, sterilizationBatchNo)
          ps.setLong(33, effectiveDate)
          ps.setLong(34, producedDate)
          ps.setString(35, warehouseName)
          ps.setString(36, supplierName)
          ps.setString(37, manufacturerName)
          ps.setString(38, commodityName)
          ps.setString(39, commoditySpec)
          ps.setString(40, classifyIdLevel1)
          ps.setString(41, classifyIdLevel2)
          ps.setString(42, brandName)
          ps.setString(43, registerName)
          ps.setString(44, logicAreaName)
          ps.setString(45, packageUnitName)
          ps.setString(46, commodityCode)
          ps.setString(47, customerName)
          ps.setString(48, commodityNumber)
          ps.setLong(49, sumInventoryNumber)

          ps.execute()
        }

        // 用户关闭连接
        override def close(errorOrNull: Throwable): Unit = {
          println("close...")
          ps.close()
          conn.close()
        }
      })
//      .option("checkpointLocation", "/Users/Administrator/Desktop/bigData/data/checkpoint/getBatchNo")
      .start()
      .awaitTermination()

    // 打印数据，查看是否正常接收数据
//    result.writeStream
//      .outputMode("update")
//      .format("console")
//      .start
//      .awaitTermination()
  }

}
