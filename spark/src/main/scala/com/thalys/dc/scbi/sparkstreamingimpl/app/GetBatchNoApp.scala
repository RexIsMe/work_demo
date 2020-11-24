package com.thalys.dc.scbi.sparkstreamingimpl.app

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.thalys.dc.scbi.sparkstreamingimpl.entity.{CaseDCRecordEntity, CaseStockWideEntity}
import com.thalys.dc.scbi.structruestreamingimpl.util.Md5
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable

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

  def statBlackList(dcRecordDS: DStream[CaseDCRecordEntity]): Unit = {
    //先将流解析成明细宽表
    val value: DStream[CaseStockWideEntity] = dcRecordDS
      .flatMap(row => {
        val entities = row.dataList
        val stockWideEntityArr: mutable.ArraySeq[CaseStockWideEntity] = entities.map(v => {
          new CaseStockWideEntity(row.ip, row.timestamp, row.companyName, row.businessName,  v.accountingMl,v.manufacturerName,v.customerEnableCode,v.ownerId,v.warehouseCategoryCode,v.organizationId,v.sterilizationBatchNo,v.inventoryNumber,v.effectiveDays, v.packageUnitName: String,v.warehouseAreaDesc, v.commodityCode, v.registerNumber, v.warehouseCategoryId, v.batchNo, v.brandName, v.commoditySpec, v.commodityNumber, v.organizationName, v.supplierEnableCode, v.logicAreaId, v.commodityType, v.productionPlace, v.professionalGroup, v.customerName, v.quotiety, v.warehouseId, v.goodsLocationId, v.storageCondition, v.inventoryOrganizationCode, v.productionLicense, v.producedDate, v.supplierId, v.warehouseAreaCode, v.accountingName, v.ownerCode, v.allocatedQuantity, v.packageUnitId, v.customerCode, v.supplierCode, v.warehouseName, v.warehouseCode, v.inventoryOrganizationName, v.goodsLocation, v.ownerName, v.isSimplelevy, v.logicAreaName, v.supplierName, v.quantity, v.inventoryOrganizationId, v.deviceClassifyType, v.warehouseCategoryName, v.commodityId, v.commodityRemark, v.deviceClassify, v.warehouseAreaId, v.logicAreaCode, v.coldChainMark, v.accountingOne, v.classifyIdLevel1, v.classifyIdLevel2, v.registerName, v.effectiveDate, v.commodityName)
        })
        stockWideEntityArr
      })

    //对宽表根据指定字段做聚合操作
    val aggregate: DStream[(CaseStockWideEntity, Integer)] = value.map(v => {
      (getKeyOfGroupBy(v), (v, v.inventoryNumber))
    }).reduceByKey((v1, v2) => {
      val sum = v1._2 + v2._2
      (v2._1, sum)
    }).map(v => {
      v._2
    })

    //将聚合结果写入到mysql
    aggregate.foreachRDD(rdd => {
      //每个rdd中包含的数据类型为(String,Int)
      //我们把所有数据records定义为Iterator类型，方便我们遍历
      def func(records:Iterator[(CaseStockWideEntity,Integer)]): Unit ={
        println(records)

        //注意，conn和stmt定义为var不能是val
        var conn: Connection = null
        var stmt : PreparedStatement = null
        try{
          //连接数据库
          val url = "jdbc:mysql://172.26.55.109:3306/dc?useUnicode=true&characterEncoding=utf8" //地址+数据库
          val user = "root"
          val password = "t4*9&/y?c,h.e17!"
          conn = DriverManager.getConnection(url,user,password)
          //
          records.foreach(p =>{
            //wordcount为表名，word和count为要插入数据的属性
            //插入数据
            // 插入数据, 当有重复的 key 的时候更新
            val sql ="""
                       |insert into ads_stock_realtime(
                       |md5_key,owner_name,goods_location,inventory_organization_id,register_number,batch_no,sterilization_batch_no,effective_date,produced_date,warehouse_name,supplier_name,manufacturer_name,commodity_name,commodity_spec,classify_id_level1,classify_id_level2,brand_name,register_name,logic_area_name,package_unit_name,commodity_code,customer_name,commodity_number,sum_inventory_number,sum_inventory_number_realtime)
                       |values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                       |on duplicate key
                       |update md5_key=?, owner_name =  ?, goods_location =  ?, inventory_organization_id =  ?, register_number =  ?, batch_no =  ?, sterilization_batch_no =  ?, effective_date =  ?, produced_date =  ?, warehouse_name =  ?, supplier_name =  ?, manufacturer_name =  ?, commodity_name =  ?, commodity_spec =  ?, classify_id_level1 =  ?, classify_id_level2 =  ?, brand_name =  ?, register_name =  ?, logic_area_name =  ?, package_unit_name =  ?, commodity_code =  ?, customer_name =  ?, commodity_number =  ?, sum_inventory_number =  sum_inventory_number,
                       | sum_inventory_number_realtime = sum_inventory_number_realtime + ?
                     """.stripMargin
            stmt = conn.prepareStatement(sql)

            val ownerName: String = p._1.ownerName
            var goodsLocation: String = p._1.goodsLocation
            var inventoryOrganizationId: String = p._1.inventoryOrganizationId
            var registerNumber: String = p._1.registerNumber
            var batchNo: String = p._1.batchNo
            var sterilizationBatchNo: String = p._1.sterilizationBatchNo
            var effectiveDate: Long = p._1.effectiveDate
            var producedDate: Long = p._1.producedDate
            var warehouseName: String = p._1.warehouseName
            var supplierName: String = p._1.supplierName
            var manufacturerName: String = p._1.manufacturerName
            var commodityName: String = p._1.commodityName
            var commoditySpec: String = p._1.commoditySpec
            var classifyIdLevel1: String = p._1.classifyIdLevel1
            var classifyIdLevel2: String = p._1.classifyIdLevel2
            var brandName: String = p._1.brandName
            var registerName: String = p._1.registerName
            var logicAreaName: String = p._1.logicAreaCode
            var packageUnitName: String = p._1.packageUnitName
            var commodityCode: String = p._1.commodityCode
            var customerName: String = p._1.customerName
            var commodityNumber: String = p._1.commodityNumber
            var sumInventoryNumber: Long = p._2.toLong
            var sumInventoryNumberRealtime: Long = sumInventoryNumber

            val md5Key: String = Md5.hashMD5(ownerName + goodsLocation + inventoryOrganizationId + registerNumber + batchNo + sterilizationBatchNo + effectiveDate.toString + producedDate.toString + warehouseName + supplierName + manufacturerName.toString + commodityName.toString + commoditySpec.toString + classifyIdLevel1.toString + classifyIdLevel2.toString + brandName.toString + registerName.toString + logicAreaName.toString + packageUnitName.toString + commodityCode.toString + customerName.toString + commodityNumber)


            stmt.setString(1, md5Key)
            stmt.setString(2, ownerName)
            stmt.setString(3, goodsLocation)
            stmt.setString(4, inventoryOrganizationId)
            stmt.setString(5, registerNumber)
            stmt.setString(6, batchNo)
            stmt.setString(7, sterilizationBatchNo)
            stmt.setLong(8, effectiveDate)
            stmt.setLong(9, producedDate)
            stmt.setString(10, warehouseName)
            stmt.setString(11, supplierName)
            stmt.setString(12, manufacturerName)
            stmt.setString(13, commodityName)
            stmt.setString(14, commoditySpec)
            stmt.setString(15, classifyIdLevel1)
            stmt.setString(16, classifyIdLevel2)
            stmt.setString(17, brandName)
            stmt.setString(18, registerName)
            stmt.setString(19, logicAreaName)
            stmt.setString(20, packageUnitName)
            stmt.setString(21, commodityCode)
            stmt.setString(22, customerName)
            stmt.setString(23, commodityNumber)
            stmt.setLong(24, sumInventoryNumber)
            stmt.setLong(25, sumInventoryNumberRealtime)

            stmt.setString(26, md5Key)
            stmt.setString(27, ownerName)
            stmt.setString(28, goodsLocation)
            stmt.setString(29, inventoryOrganizationId)
            stmt.setString(30, registerNumber)
            stmt.setString(31, batchNo)
            stmt.setString(32, sterilizationBatchNo)
            stmt.setLong(33, effectiveDate)
            stmt.setLong(34, producedDate)
            stmt.setString(35, warehouseName)
            stmt.setString(36, supplierName)
            stmt.setString(37, manufacturerName)
            stmt.setString(38, commodityName)
            stmt.setString(39, commoditySpec)
            stmt.setString(40, classifyIdLevel1)
            stmt.setString(41, classifyIdLevel2)
            stmt.setString(42, brandName)
            stmt.setString(43, registerName)
            stmt.setString(44, logicAreaName)
            stmt.setString(45, packageUnitName)
            stmt.setString(46, commodityCode)
            stmt.setString(47, customerName)
            stmt.setString(48, commodityNumber)
            stmt.setLong(49, sumInventoryNumber)

            stmt.executeUpdate()
          })
        }catch {
          case e : Exception => e.printStackTrace()
        }finally {
          if(stmt != null)
            stmt.close()
          if(conn != null)
            conn.close()
        }
      }
      val repairtitionedRDD = rdd.repartition(3)//将每个rdd重新分区
      repairtitionedRDD.foreachPartition(func)//对重新分区后的rdd执行func函数
    })

    // 打印数据，查看是否正常接收数据
//    .print()
  }

  def getKeyOfGroupBy(swe: CaseStockWideEntity): String ={
    val key = swe.ownerName + swe.goodsLocation
    key
  }

}
