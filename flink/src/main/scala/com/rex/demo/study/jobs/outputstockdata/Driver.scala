package com.rex.demo.study.jobs.outputstockdata

import java.sql.PreparedStatement
import java.util.Properties

import com.fasterxml.jackson.databind.JsonNode
import com.rex.demo.study.util.JsonUtils
import org.apache.commons.lang3.StringUtils
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011

/**
  * descriptions:
  * 实时消费kafka中outputstockdata数据并解析、保存到clickhouse
  *
  * author: li zhiqiang
  * date: 2021 - 02 - 20 14:43
  */
object Driver {
  def main(args: Array[String]): Unit = {
    // 1、获取流式环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2、从文件中读取数据
    //从Kafka读取数据
    val pros = new Properties()
    pros.setProperty("bootstrap.servers", "172.16.0.211:9092")
    pros.setProperty("group.id", "outputStockData")
    pros.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    pros.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    pros.setProperty("auto.offset.reset", "latest")
    import org.apache.flink.api.scala._
    val dataStream = env.addSource(new FlinkKafkaConsumer011[String]("test", new SimpleStringSchema(), pros))
      .map(line => {
        val rootNode: JsonNode = JsonUtils.getRootNode(line)
        val ip = rootNode.path("ip").asText()
        val timestamp = rootNode.path("timestamp").asLong()
        val businessName = rootNode.path("businessName").asText()
        val companyName = rootNode.path("companyName").asText()
        val dataList: JsonNode = rootNode.path("dataList")
        val stockDataJson: JsonNode = dataList.get(0)

        val stockData: StockData = JsonUtils.toObject[StockData](stockDataJson.toString, classOf[StockData])
        StockDataFull(ip, timestamp, businessName, companyName, stockData)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[StockDataFull](Time.seconds(5)) {
        override def extractTimestamp(element: StockDataFull): Long = element.timestamp
      })

    val skinSql = "insert into test.stock_data(ip ," + "time_stamp ," + "business_name ," + "company_name ," + "batch_no ," + "inventory_number ," + "warehouse_name ," + "warehouse_id ," + "warehouse_code ," + "warehouse_category_name ," + "warehouse_category_id ," + "warehouse_category_code ," + "warehouse_area_id ," + "warehouse_area_desc ," + "warehouse_area_code ," + "supplier_name ," + "supplier_id ," + "supplier_enable_code ," + "supplier_code ," + "storage_condition ," + "sterilization_batch_no ," + "register_number ," + "register_name ," + "quotiety ," + "quantity ," + "professional_group ," + "production_place ," + "production_license ," + "produced_date ," + "package_unit_name ," + "accounting_ml ," + "accounting_name ," + "accounting_one ," + "allocated_quantity ," + "brand_name ," + "classify_id_level1 ," + "classify_id_level2 ," + "cold_chain_mark ," + "commodity_code ," + "commodity_id ," + "commodity_name ," + "commodity_number ," + "commodity_remark ," + "commodity_spec ," + "commodity_type ," + "customer_code ," + "customer_enable_code ," + "customer_id ," + "customer_name ," + "device_classify ," + "device_classify_type ," + "effective_date ," + "effective_days ," + "goods_location ," + "goods_location_id ," + "inventory_organization_code ," + "inventory_organization_id ," + "inventory_organization_name ," + "is_simplelevy ," + "logic_area_code ," + "logic_area_id ," + "logic_area_name ," + "manufacturer_name ," + "organization_id ," + "organization_name ," + "owner_code ," + "owner_id ," + "owner_name ," + "package_unitId ) values(?,?,?,?,?,?,?,?,?,?," + "?,?,?,?,?,?,?,?,?,?," + "?,?,?,?,?,?,?,?,?,?," + "?,?,?,?,?,?,?,?,?,?," + "?,?,?,?,?,?,?,?,?,?," + "?,?,?,?,?,?,?,?,?,?," + "?,?,?,?,?,?,?,?,?);"
    // 3、 skin ClickHouse
    dataStream.addSink(
      JdbcSink.sink(
        skinSql,
        new CHSinkBuilder,
        new JdbcExecutionOptions.Builder()
          .withBatchSize(1)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUrl("jdbc:clickhouse://172.16.0.213:8123/test")
//          .withUsername("admin")
//          .withPassword("123456")
          .build()
      )
    )
    // 4、执行任务
    env.execute("SinkCH Job")
  }

  //用户信息样例类
  case class StockData( accountingMl: String,
                        manufacturerName: String,
                        customerEnableCode: String,
                        ownerId: Int,
                        warehouseCategoryCode: String,
                        organizationId: Long,
                        inventoryNumber: Int,
                        sterilizationBatchNo: String,
                        effectiveDays: Int,
                        packageUnitName: String,
                        warehouseAreaDesc: String,
                        commodityCode: String,
                        registerNumber: String,
                        warehouseCategoryId: Int,
                        batchNo: String,
                        brandName: String,
                        commoditySpec: String,
                        commodityNumber: String,
                        organizationName: String,
                        supplierEnableCode: String,
                        logicAreaId: Int,
                        commodityType: String,
                        productionPlace: String,
                        professionalGroup: String,
                        customerName: String,
                        quotiety: Int,
                        warehouseId: Int,
                        goodsLocationId: Int,
                        storageCondition: String,
                        inventoryOrganizationCode: String,
                        productionLicense: String,
                        producedDate: Long,
                        supplierId: Int,
                        warehouseAreaCode: String,
                        accountingName: String,
                        ownerCode: String,
                        allocatedQuantity: Int,
                        packageUnitId: Int,
                        customerCode: String,
                        customerId: Int,
                        supplierCode: String,
                        warehouseName: String,
                        warehouseCode: String,
                        inventoryOrganizationName: String,
                        goodsLocation: String,
                        ownerName: String,
                        isSimplelevy: String,
                        logicAreaName: String,
                        supplierName: String,
                        quantity: Int,
                        inventoryOrganizationId: String,
                        deviceClassifyType: String,
                        warehouseCategoryName: String,
                        commodityId: Int,
                        commodityRemark: String,
                        deviceClassify: String,
                        warehouseAreaId: Int,
                        logicAreaCode: String,
                        coldChainMark: String,
                        accountingOne: String,
                        classifyIdLevel1: String,
                        classifyIdLevel2: String,
                        registerName: String,
                        effectiveDate: Long,
                        commodityName: String)
  case class StockDataFull(ip: String, timestamp: Long, companyName: String, businessName: String, stockData: StockData)
  //重写JdbcStatementBuilder
  class CHSinkBuilder extends JdbcStatementBuilder[StockDataFull] {
    override def accept(ps: PreparedStatement, stockEntity: StockDataFull): Unit = {
      ps.setString(1, stockEntity.ip)
      ps.setLong(2, stockEntity.timestamp)
      ps.setString(3, stockEntity.businessName)
      ps.setString(4, stockEntity.companyName)
      ps.setString(5, StringUtils.trimToEmpty(stockEntity.stockData.batchNo))
      ps.setInt(6, stockEntity.stockData.inventoryNumber)
      ps.setString(7, stockEntity.stockData.warehouseName)
      ps.setInt(8, stockEntity.stockData.warehouseId)
      ps.setString(9, stockEntity.stockData.warehouseCode)
      ps.setString(10, stockEntity.stockData.warehouseCategoryName)
      ps.setInt(11, stockEntity.stockData.warehouseCategoryId)
      ps.setString(12, stockEntity.stockData.warehouseCategoryCode)
      ps.setInt(13, stockEntity.stockData.warehouseAreaId)
      ps.setString(14, stockEntity.stockData.warehouseAreaDesc)
      ps.setString(15, stockEntity.stockData.warehouseAreaCode)
      ps.setString(16, stockEntity.stockData.supplierName)
      ps.setInt(17, stockEntity.stockData.supplierId)
      ps.setString(18, stockEntity.stockData.supplierEnableCode)
      ps.setString(19, stockEntity.stockData.supplierCode)
      ps.setString(20, stockEntity.stockData.storageCondition)
      ps.setString(21, stockEntity.stockData.sterilizationBatchNo)
      ps.setString(22, stockEntity.stockData.registerNumber)
      ps.setString(23, stockEntity.stockData.registerName)
      ps.setInt(24, stockEntity.stockData.quotiety)
      ps.setInt(25, stockEntity.stockData.quantity)
      ps.setString(26, stockEntity.stockData.professionalGroup)
      ps.setString(27, stockEntity.stockData.productionPlace)
      ps.setString(28, stockEntity.stockData.productionLicense)
      ps.setLong(29, stockEntity.stockData.producedDate)
      ps.setString(30, stockEntity.stockData.packageUnitName)
      ps.setString(31, stockEntity.stockData.accountingMl)
      ps.setString(32, stockEntity.stockData.accountingName)
      ps.setString(33, stockEntity.stockData.accountingOne)
      ps.setInt(34, stockEntity.stockData.allocatedQuantity)
      ps.setString(35, stockEntity.stockData.brandName)
      ps.setString(36, stockEntity.stockData.classifyIdLevel1)
      ps.setString(37, stockEntity.stockData.classifyIdLevel2)
      ps.setString(38, stockEntity.stockData.coldChainMark)
      ps.setString(39, stockEntity.stockData.commodityCode)
      ps.setInt(40, stockEntity.stockData.commodityId)
      ps.setString(41, stockEntity.stockData.commodityName)
      ps.setString(42, stockEntity.stockData.commodityNumber)
      ps.setString(43, stockEntity.stockData.commodityRemark)
      ps.setString(44, stockEntity.stockData.commoditySpec)
      ps.setString(45, stockEntity.stockData.commodityType)
      ps.setString(46, stockEntity.stockData.customerCode)
      ps.setString(47, stockEntity.stockData.customerEnableCode)
      ps.setInt(48, if (stockEntity.stockData.customerId == null) 0 else stockEntity.stockData.customerId)
      ps.setString(49, stockEntity.stockData.customerName)
      ps.setString(50, stockEntity.stockData.deviceClassify)
      ps.setString(51, stockEntity.stockData.deviceClassifyType)
      ps.setLong(52, stockEntity.stockData.effectiveDate)
      ps.setInt(53, stockEntity.stockData.effectiveDays)
      ps.setString(54, stockEntity.stockData.goodsLocation)
      ps.setInt(55, stockEntity.stockData.goodsLocationId)
      ps.setString(56, stockEntity.stockData.inventoryOrganizationCode)
      ps.setString(57, stockEntity.stockData.inventoryOrganizationId)
      ps.setString(58, stockEntity.stockData.inventoryOrganizationName)
      ps.setString(59, stockEntity.stockData.isSimplelevy)
      ps.setString(60, stockEntity.stockData.logicAreaCode)
      ps.setInt(61, stockEntity.stockData.logicAreaId)
      ps.setString(62, stockEntity.stockData.logicAreaName)
      ps.setString(63, stockEntity.stockData.manufacturerName)
      ps.setLong(64, stockEntity.stockData.organizationId)
      ps.setString(65, stockEntity.stockData.organizationName)
      ps.setString(66, stockEntity.stockData.ownerCode)
      ps.setInt(67, stockEntity.stockData.ownerId)
      ps.setString(68, stockEntity.stockData.ownerName)
      ps.setInt(69, stockEntity.stockData.packageUnitId)
    }
  }

}

