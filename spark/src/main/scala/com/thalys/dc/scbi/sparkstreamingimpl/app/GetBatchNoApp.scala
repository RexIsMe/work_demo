package com.thalys.dc.scbi.sparkstreamingimpl.app

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.thalys.dc.scbi.sparkstreamingimpl.entity.{DCRecordEntity, StockWildEntity}
import com.thalys.dc.scbi.structruestreamingimpl.util.Md5
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.streaming.dstream.DStream

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

  def statBlackList(dcRecordDS: DStream[DCRecordEntity]): Unit = {
    //先将流解析成明细表
    val stockWildDS: DStream[StockWildEntity] = dcRecordDS
      .flatMap(row => {
        val list = row.dataList.toList
        val entities: List[StockWildEntity] = list.map(v => StockWildEntity(row.ip, row.timestamp, row.companyName, row.businessName, v.accountingMl, v.manufacturerName, v.customerEnableCode, v.ownerId, v.warehouseCategoryCode, v.organizationId, v.sterilizationBatchNo, v.inventoryNumber, v.effectiveDays, v.packageUnitName: String, v.warehouseAreaDesc, v.commodityCode, v.registerNumber, v.warehouseCategoryId, v.batchNo, v.brandName, v.commoditySpec, v.commodityNumber, v.organizationName, v.supplierEnableCode, v.logicAreaId, v.commodityType, v.productionPlace, v.professionalGroup, v.customerName, v.quotiety, v.warehouseId, v.goodsLocationId, v.storageCondition, v.inventoryOrganizationCode, v.productionLicense, v.producedDate, v.supplierId, v.warehouseAreaCode, v.accountingName, v.ownerCode, v.allocatedQuantity, v.packageUnitId, v.customerCode, v.supplierCode, v.warehouseName, v.warehouseCode, v.inventoryOrganizationName, v.goodsLocation, v.ownerName, v.isSimplelevy, v.logicAreaName, v.supplierName, v.quantity, v.inventoryOrganizationId, v.deviceClassifyType, v.warehouseCategoryName, v.commodityId, v.commodityRemark, v.deviceClassify, v.warehouseAreaId, v.logicAreaCode, v.coldChainMark, v.accountingOne, v.classifyIdLevel1, v.classifyIdLevel2, v.registerName, v.effectiveDate, v.commodityName))
        entities
      })



    // 打印数据，查看是否正常接收数据
//    dcRecordDS.print()
    stockWildDS.print()
  }

}
