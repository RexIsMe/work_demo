package com.thalys.dc.scbi.structruestreamingimpl.entity

import java.sql.Timestamp

import com.fasterxml.jackson.annotation.JsonIgnoreProperties

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 09 15:24
  */
@JsonIgnoreProperties(ignoreUnknown = true)
case class StockEntity (
accountingMl: String,
manufacturerName: String,
customerEnableCode: String,
ownerId: Integer,
warehouseCategoryCode: String,
organizationId: Long,
sterilizationBatchNo: String,
var inventoryNumber: Integer,
effectiveDays: Integer,
packageUnitName: String,
warehouseAreaDesc: String,
commodityCode: String,
registerNumber: String,
warehouseCategoryId: Integer,
batchNo: String,
brandName: String,
commoditySpec: String,
commodityNumber: String,
organizationName: String,
supplierEnableCode: String,
logicAreaId: Integer,
commodityType: String,
productionPlace: String,
professionalGroup: String,
customerName: String,
quotiety: Integer,
warehouseId: Integer,
goodsLocationId: Integer,
storageCondition: String,
inventoryOrganizationCode: String,
productionLicense: String,
producedDate: Long,
supplierId: Integer,
warehouseAreaCode: String,
accountingName: String,
ownerCode: String,
allocatedQuantity: Integer,
packageUnitId: Integer,
customerCode: String,
supplierCode: String,
warehouseName: String,
warehouseCode: String,
inventoryOrganizationName: String,
goodsLocation: String,
ownerName: String,
isSimplelevy: String,
logicAreaName: String,
supplierName: String,
quantity: Integer,
inventoryOrganizationId: String,
deviceClassifyType: String,
warehouseCategoryName: String,
commodityId: Integer,
commodityRemark: String,
deviceClassify: String,
warehouseAreaId: Integer,
logicAreaCode: String,
coldChainMark: String,
accountingOne: String,
classifyIdLevel1: String,
classifyIdLevel2: String,
registerName: String,
effectiveDate: Long,
commodityName: String
){
  override def equals(obj: Any): Boolean = {
    var result:Boolean = false
    if(this.customerEnableCode.equals(obj.asInstanceOf[StockEntity].customerEnableCode)){
      result = true
    }
    result
  }
}
