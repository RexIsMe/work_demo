package com.rex.demo.study.jobs.outputstockdata

import java.lang
import java.sql.PreparedStatement

import com.fasterxml.jackson.databind.JsonNode
import com.rex.demo.study.demo.util.CommonUtils
import com.rex.demo.study.util.JsonUtils
import lombok.Data
import org.apache.commons.lang3.StringUtils
import org.apache.flink.connector.jdbc._
import org.apache.flink.streaming.api.scala._

/**
  * descriptions:
  * 将生产环境ES现有的outputstockdata数据解析、导入到clickhouse
  *
  * author: li zhiqiang
  * date: 2021 - 02 - 20 14:43
  */
object ReadJsonFileDriver {
  def main(args: Array[String]): Unit = {
    // 1、获取流式环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 2、从文件中读取数据
    //从json文件中读取数据
//    val dataStream = env.readTextFile(CommonUtils.getResourcePath("OutputStockDataTest.json"))
    val dataStream = env.readTextFile("C:\\Users\\Administrator\\Desktop\\bigData\\clickhouse\\wms-outputstockdata-0209.json")
      .map(line => {
        val rootNode: JsonNode = JsonUtils.getRootNode(line)
        val ip = ""
        val timestamp = System.currentTimeMillis()
        val businessName = "stock"
        val companyName = "thalys"
        val _source: JsonNode = rootNode.path("_source")
        val _id: String = rootNode.path("_id").asText()
        val outputStockData: OutputStockData = JsonUtils.toObject[OutputStockData](_source.toString, classOf[OutputStockData])
        StockDataFull(ip, timestamp, businessName, companyName, _id, outputStockData)
      })

    val skinSql = "insert into test.output_stock_data( ip , time_stamp , business_name , company_name , batch_no , inventory_number , warehouse_name , warehouse_id , warehouse_code , warehouse_category_name , warehouse_category_id , warehouse_category_code , warehouse_area_id , warehouse_area_desc , warehouse_area_code , supplier_name , supplier_id , supplier_enable_code , supplier_code , storage_condition , sterilization_batch_no , register_number , register_name , quotiety , quantity , professional_group , production_place , production_license , produced_date , package_unit_name , accounting_ml , accounting_name , accounting_one , allocated_quantity , brand_name , classify_id_level1 , classify_id_level2 , cold_chain_mark , commodity_code , commodity_id , commodity_name , commodity_number , commodity_remark , commodity_spec , commodity_type , customer_code , customer_enable_code , customer_id , customer_name , device_classify , device_classify_type , effective_date , effective_days , goods_location , goods_location_id , inventory_organization_code , inventory_organization_id , inventory_organization_name , is_simplelevy , logic_area_code , logic_area_id , logic_area_name , manufacturer_name , organization_id , organization_name , owner_code , owner_id , owner_name , package_unitId , id, order_item_no, win_bid_code, invoice_unit_name, invoice_name, customer_commodity_remark, sale_price, currency, tax, taxed_price, create_user, create_time, update_user, update_time, receiving_user_id, receiving_user, receiving_date, acceptance_user_id, acceptance_user, acceptance_date, recheck_user_id, recheck_user, recheck_date, put_away_id, put_away_user, put_away_date, order_type_id, order_type_name, receive_number, purchase_number, commodity_date, purchase_order_item_no, purchase_people, order_no, recheck_quantity ) values(?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?,?,?,?,?,?,?, ?,?,?,?); "
    // 3、 skin ClickHouse
    dataStream.addSink(
      JdbcSink.sink(
        skinSql,
        new CHSinkBuilder,
        new JdbcExecutionOptions.Builder()
          .withBatchSize(200)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUrl("jdbc:clickhouse://172.16.0.213:8123/test")
//          .withUsername("admin")
//          .withPassword("123456")
          .build()
      )
    )

//    dataStream.addSink(new SinkToMySQL())

    // 4、执行任务
    env.execute("SinkCH Job")
  }

  //用户信息样例类
  @Data
  case class OutputStockData( accountingMl: String,
                        manufacturerName: String,
                        customerEnableCode: String,
                        ownerId: Long,
                        warehouseCategoryCode: String,
                        organizationId: Long,
                        inventoryNumber: Long,
                        sterilizationBatchNo: String,
                        effectiveDays: Long,
                        packageUnitName: String,
                        warehouseAreaDesc: String,
                        commodityCode: String,
                        registerNumber: String,
                        warehouseCategoryId: Long,
                        batchNo: String,
                        brandName: String,
                        commoditySpec: String,
                        commodityNumber: String,
                        organizationName: String,
                        supplierEnableCode: String,
                        logicAreaId: Long,
                        commodityType: String,
                        productionPlace: String,
                        professionalGroup: String,
                        customerName: String,
                        quotiety: Long,
                        warehouseId: Long,
                        goodsLocationId: Long,
                        storageCondition: String,
                        inventoryOrganizationCode: String,
                        productionLicense: String,
                        producedDate: Long,
                        supplierId: Long,
                        warehouseAreaCode: String,
                        accountingName: String,
                        ownerCode: String,
                        allocatedQuantity: Long,
                        packageUnitId: Long,
                        customerCode: String,
                        customerId: Long,
                        supplierCode: String,
                        warehouseName: String,
                        warehouseCode: String,
                        inventoryOrganizationName: String,
                        goodsLocation: String,
                        ownerName: String,
                        isSimplelevy: String,
                        logicAreaName: String,
                        supplierName: String,
                        quantity: Long,
                        inventoryOrganizationId: String,
                        deviceClassifyType: String,
                        warehouseCategoryName: String,
                        commodityId: Long,
                        commodityRemark: String,
                        deviceClassify: String,
                        warehouseAreaId: Long,
                        logicAreaCode: String,
                        coldChainMark: String,
                        accountingOne: String,
                        classifyIdLevel1: String,
                        classifyIdLevel2: String,
                        registerName: String,
                        effectiveDate: Long,
                        commodityName: String,
                        id: String,

                        orderItemNo: String,
                        winBidCode: String,
                        invoiceUnitName: String,
                        invoiceName: String,
                        customerCommodityRemark: String,
                        salePrice: java.math.BigDecimal,
                        currency: String,
                        tax: String,
                        taxedPrice: java.math.BigDecimal,
                        createUser: String,
                        createTime: Long,
                        updateUser: String,
                        updateTime: Long,
                        receivingUserId: Long,
                        receivingUser: String,
                        receivingDate: Long,
                        acceptanceUserId: Long,
                        acceptanceUser: String,
                        acceptanceDate: Long,
                        recheckUserId: Long,
                        recheckUser: String,
                        recheckDate: Long,
                        putawayId: Long,
                        putawayUser: String,
                        putawayDate: Long,
                        orderTypeId: Long,
                        orderTypeName: String,
                        receiveNumber: java.math.BigDecimal,
                        purchaseNumber: java.math.BigDecimal,
                        commodityDate: Long,
                        purchaseOrderItemNo: String,
                        purchasePeople: String,
                        orderNo: String,
                        recheckQuantity: java.math.BigDecimal
                            )

  case class StockDataFull(ip: String, timestamp: Long, companyName: String, businessName: String, id: String, outputStockData: OutputStockData)
  //重写JdbcStatementBuilder
  class CHSinkBuilder extends JdbcStatementBuilder[StockDataFull] {
    override def accept(ps: PreparedStatement, stockEntity: StockDataFull): Unit = {
      ps.setString(1, stockEntity.ip)
      ps.setLong(2, stockEntity.timestamp)
      ps.setString(3, stockEntity.businessName)
      ps.setString(4, stockEntity.companyName)
      ps.setString(5, StringUtils.trimToEmpty(stockEntity.outputStockData.batchNo))
      ps.setLong(6, stockEntity.outputStockData.inventoryNumber)
      ps.setString(7, stockEntity.outputStockData.warehouseName)
      ps.setLong(8, stockEntity.outputStockData.warehouseId)
      ps.setString(9, stockEntity.outputStockData.warehouseCode)
      ps.setString(10, stockEntity.outputStockData.warehouseCategoryName)
      ps.setLong(11, stockEntity.outputStockData.warehouseCategoryId)
      ps.setString(12, stockEntity.outputStockData.warehouseCategoryCode)
      ps.setLong(13, stockEntity.outputStockData.warehouseAreaId)
      ps.setString(14, stockEntity.outputStockData.warehouseAreaDesc)
      ps.setString(15, stockEntity.outputStockData.warehouseAreaCode)
      ps.setString(16, stockEntity.outputStockData.supplierName)
      ps.setLong(17, stockEntity.outputStockData.supplierId)
      ps.setString(18, stockEntity.outputStockData.supplierEnableCode)
      ps.setString(19, stockEntity.outputStockData.supplierCode)
      ps.setString(20, stockEntity.outputStockData.storageCondition)
      ps.setString(21, stockEntity.outputStockData.sterilizationBatchNo)
      ps.setString(22, stockEntity.outputStockData.registerNumber)
      ps.setString(23, stockEntity.outputStockData.registerName)
      ps.setLong(24, stockEntity.outputStockData.quotiety)
      ps.setLong(25, stockEntity.outputStockData.quantity)
      ps.setString(26, stockEntity.outputStockData.professionalGroup)
      ps.setString(27, stockEntity.outputStockData.productionPlace)
      ps.setString(28, stockEntity.outputStockData.productionLicense)
      ps.setLong(29, stockEntity.outputStockData.producedDate)
      ps.setString(30, stockEntity.outputStockData.packageUnitName)
      ps.setString(31, stockEntity.outputStockData.accountingMl)
      ps.setString(32, stockEntity.outputStockData.accountingName)
      ps.setString(33, stockEntity.outputStockData.accountingOne)
      ps.setLong(34, stockEntity.outputStockData.allocatedQuantity)
      ps.setString(35, stockEntity.outputStockData.brandName)
      ps.setString(36, stockEntity.outputStockData.classifyIdLevel1)
      ps.setString(37, stockEntity.outputStockData.classifyIdLevel2)
      ps.setString(38, stockEntity.outputStockData.coldChainMark)
      ps.setString(39, stockEntity.outputStockData.commodityCode)
      ps.setLong(40, stockEntity.outputStockData.commodityId)
      ps.setString(41, stockEntity.outputStockData.commodityName)
      ps.setString(42, stockEntity.outputStockData.commodityNumber)
      ps.setString(43, stockEntity.outputStockData.commodityRemark)
      ps.setString(44, stockEntity.outputStockData.commoditySpec)
      ps.setString(45, stockEntity.outputStockData.commodityType)
      ps.setString(46, stockEntity.outputStockData.customerCode)
      ps.setString(47, stockEntity.outputStockData.customerEnableCode)
      ps.setLong(48, if (stockEntity.outputStockData.customerId == null) 0 else stockEntity.outputStockData.customerId)
      ps.setString(49, stockEntity.outputStockData.customerName)
      ps.setString(50, stockEntity.outputStockData.deviceClassify)
      ps.setString(51, stockEntity.outputStockData.deviceClassifyType)
      ps.setLong(52, stockEntity.outputStockData.effectiveDate)
      ps.setLong(53, stockEntity.outputStockData.effectiveDays)
      ps.setString(54, stockEntity.outputStockData.goodsLocation)
      ps.setLong(55, stockEntity.outputStockData.goodsLocationId)
      ps.setString(56, stockEntity.outputStockData.inventoryOrganizationCode)
      ps.setString(57, stockEntity.outputStockData.inventoryOrganizationId)
      ps.setString(58, stockEntity.outputStockData.inventoryOrganizationName)
      ps.setString(59, stockEntity.outputStockData.isSimplelevy)
      ps.setString(60, stockEntity.outputStockData.logicAreaCode)
      ps.setLong(61, stockEntity.outputStockData.logicAreaId)
      ps.setString(62, stockEntity.outputStockData.logicAreaName)
      ps.setString(63, stockEntity.outputStockData.manufacturerName)
      ps.setLong(64, stockEntity.outputStockData.organizationId)
      ps.setString(65, stockEntity.outputStockData.organizationName)
      ps.setString(66, stockEntity.outputStockData.ownerCode)
      ps.setLong(67, stockEntity.outputStockData.ownerId)
      ps.setString(68, stockEntity.outputStockData.ownerName)
      ps.setLong(69, stockEntity.outputStockData.packageUnitId)
      ps.setString(70, stockEntity.id)

      ps.setString(71, stockEntity.outputStockData.orderItemNo)
      ps.setString(72, stockEntity.outputStockData.winBidCode)
      ps.setString(73, stockEntity.outputStockData.invoiceUnitName)
      ps.setString(74, stockEntity.outputStockData.invoiceName)
      ps.setString(75, stockEntity.outputStockData.customerCommodityRemark)
      ps.setBigDecimal(76, if(stockEntity.outputStockData.salePrice == null ) new java.math.BigDecimal(0.0) else stockEntity.outputStockData.salePrice)
      ps.setString(77, stockEntity.outputStockData.currency)
      ps.setString(78, stockEntity.outputStockData.tax)
      ps.setBigDecimal(79, if(stockEntity.outputStockData.taxedPrice == null ) new java.math.BigDecimal(0.0) else stockEntity.outputStockData.taxedPrice)
      ps.setString(80, stockEntity.outputStockData.createUser)
      ps.setLong(81, stockEntity.outputStockData.createTime)
      ps.setString(82, stockEntity.outputStockData.updateUser)
      ps.setLong(83, stockEntity.outputStockData.updateTime)
      ps.setLong(84, stockEntity.outputStockData.receivingUserId)
      ps.setString(85, stockEntity.outputStockData.receivingUser)
      ps.setLong(86, stockEntity.outputStockData.receivingDate)
      ps.setLong(87, stockEntity.outputStockData.acceptanceUserId)
      ps.setString(88, stockEntity.outputStockData.acceptanceUser)
      ps.setLong(89, stockEntity.outputStockData.acceptanceDate)
      ps.setLong(90, stockEntity.outputStockData.recheckUserId)
      ps.setString(91, stockEntity.outputStockData.recheckUser)
      ps.setLong(92, stockEntity.outputStockData.recheckDate)
      ps.setLong(93, stockEntity.outputStockData.putawayId)
      ps.setString(94, stockEntity.outputStockData.putawayUser)
      ps.setLong(95, stockEntity.outputStockData.putawayDate)
      ps.setLong(96, stockEntity.outputStockData.orderTypeId)
      ps.setString(97, stockEntity.outputStockData.orderTypeName)
      ps.setBigDecimal(98, if(stockEntity.outputStockData.receiveNumber == null ) new java.math.BigDecimal(0.0) else stockEntity.outputStockData.receiveNumber)
      ps.setBigDecimal(99, if(stockEntity.outputStockData.purchaseNumber == null ) new java.math.BigDecimal(0.0) else stockEntity.outputStockData.purchaseNumber)
      ps.setLong(100, stockEntity.outputStockData.commodityDate)
      ps.setString(101, stockEntity.outputStockData.purchaseOrderItemNo)
      ps.setString(102, stockEntity.outputStockData.purchasePeople)
      ps.setString(103, stockEntity.outputStockData.orderNo)
      ps.setBigDecimal(104, if(stockEntity.outputStockData.recheckQuantity == null ) new java.math.BigDecimal(0.0) else stockEntity.outputStockData.recheckQuantity)
    }
  }

}


