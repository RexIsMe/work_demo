package com.thalys.dc.scbi.structruestreamingimpl.util

import java.util

import com.fasterxml.jackson.core.JsonParseException
import com.fasterxml.jackson.databind.node.JsonNodeType
import com.fasterxml.jackson.databind.{DeserializationFeature, JsonMappingException, JsonNode, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.thalys.dc.scbi.structruestreamingimpl.entity.StockEntity

/**
  * descriptions:
  * json相关工具类
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 10 17:01
  */
object JsonUtils {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
  mapper.disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)

  def toObject[T](content: String, classType: Class[T]): T = {
    mapper.readValue(content, classType)
  }

  def toJson(obj: Object): String = {
    try {
      mapper.writeValueAsString(obj)
    } catch {
      case e: Exception =>
        println(e)
        null
    }
  }

  def getRootNode(content: String) : JsonNode = {
    mapper.readTree(content)
  }

  def main(args: Array[String]): Unit = {
    test2
  }


  def test2 : Unit = {
    import java.io.IOException
    try {
      val mapper = new ObjectMapper()
      val jsonString = "{\"businessName\":\"stock\",\"companyName\":\"thalys.tyche\",\"dataList\":[{\"accountingMl\":\"\",\"manufacturerName\":\"贝克曼库尔特商贸(中国)有限公司\",\"customerEnableCode\":\"0.ZGS.NM.NMZX.0001\",\"ownerId\":39,\"warehouseCategoryCode\":\"QUALIFIED\",\"organizationId\":2,\"sterilizationBatchNo\":\"1\",\"inventoryNumber\":20,\"effectiveDays\":360,\"packageUnitName\":\"盒\",\"warehouseAreaDesc\":\"冷藏区\",\"commodityCode\":\"1.02.00001\",\"registerNumber\":\"国械注进20172400054\",\"warehouseCategoryId\":2,\"batchNo\":\"1\",\"brandName\":\"贝克曼\",\"commoditySpec\":\"2*50个测试/盒\",\"commodityNumber\":\"00001\",\"organizationName\":\"塞力斯医疗科技股份有限公司\",\"supplierEnableCode\":\"6.01.01.0008\",\"logicAreaId\":75,\"commodityType\":\"2137\",\"productionPlace\":\"国产\",\"professionalGroup\":\"免疫-发光\",\"customerName\":\"测试内蒙古医科大学附属人民医院CS1\",\"quotiety\":1,\"warehouseId\":53,\"goodsLocationId\":5133,\"storageCondition\":\"2-10摄氏度\",\"inventoryOrganizationCode\":\"0120\",\"productionLicense\":\"\",\"producedDate\":1600185600,\"supplierId\":5219,\"warehouseAreaCode\":\"LC\",\"accountingName\":\"\",\"ownerCode\":\"0120\",\"allocatedQuantity\":0,\"packageUnitId\":2038,\"customerCode\":\"0.ZGS.NM.NMZX.0001\",\"supplierCode\":\"6.01.01.0008\",\"warehouseName\":\"内蒙塞力斯总仓\",\"warehouseCode\":\"012001\",\"inventoryOrganizationName\":\"内蒙古塞力斯医疗科技有限公司\",\"goodsLocation\":\"LC02-021701\",\"ownerName\":\"内蒙古塞力斯医疗科技有限公司\",\"customerId\":7432,\"isSimplelevy\":\"0\",\"logicAreaName\":\"冷藏肿瘤医院器械\",\"supplierName\":\"成都瑞琦医疗科技有限责任公司\",\"quantity\":20,\"inventoryOrganizationId\":48,\"deviceClassifyType\":\"2035\",\"warehouseCategoryName\":\"合格\",\"commodityId\":53574,\"commodityRemark\":\"\",\"deviceClassify\":\"体外诊断试剂\",\"warehouseAreaId\":94,\"logicAreaCode\":\"LC_ZLYYQX\",\"coldChainMark\":\"2153\",\"accountingOne\":\"\",\"classifyIdLevel1\":\"器械类-试剂\",\"classifyIdLevel2\":\"试剂-主试剂\",\"registerName\":\"维生素B12试剂盒\",\"effectiveDate\":1631289600,\"commodityName\":\"维生素B12测定试剂盒（化学发光法）\"},{\"accountingMl\":\"\",\"manufacturerName\":\"Siemens Healthcare Diagnostics products GmbH\",\"customerEnableCode\":\"\",\"ownerId\":1,\"warehouseCategoryCode\":\"QUALIFIED\",\"organizationId\":2,\"sterilizationBatchNo\":\"\",\"inventoryNumber\":8,\"effectiveDays\":720,\"packageUnitName\":\"条\",\"warehouseAreaDesc\":\"冷藏\",\"commodityCode\":\"1.01.15.F00031\",\"registerNumber\":\"国械注进20162401163\",\"warehouseCategoryId\":2,\"batchNo\":\"081411\",\"brandName\":\"西门子\",\"commoditySpec\":\"   10*15ml\",\"commodityNumber\":\"F000311\",\"organizationName\":\"塞力斯医疗科技股份有限公司\",\"supplierEnableCode\":\"3.01.01.0226.01\",\"logicAreaId\":63,\"commodityType\":\"2137\",\"productionPlace\":\"国产\",\"professionalGroup\":\"临检-血凝\",\"customerName\":\"\",\"quotiety\":1,\"warehouseId\":41,\"goodsLocationId\":2737,\"storageCondition\":\"冷藏\",\"inventoryOrganizationCode\":\"0109\",\"productionLicense\":\"\",\"producedDate\":1579449600,\"supplierId\":5104,\"warehouseAreaCode\":\"LC\",\"accountingName\":\"耗材（含各种洗液、定标质控、一次性使用耗材等）\",\"ownerCode\":\"0109\",\"allocatedQuantity\":0,\"packageUnitId\":2040,\"customerCode\":\"\",\"supplierCode\":\"3.01.01.0226.01\",\"warehouseName\":\"奥申博仓库\",\"warehouseCode\":\"010901\",\"inventoryOrganizationName\":\"武汉奥申博科技有限公司\",\"goodsLocation\":\"LC1-010101\",\"ownerName\":\"武汉奥申博科技有限公司\",\"isSimplelevy\":\"0\",\"logicAreaName\":\"LC-冷藏区\",\"supplierName\":\"武汉逸文商贸有限公司\",\"quantity\":10,\"inventoryOrganizationId\":39,\"deviceClassifyType\":\"2035\",\"warehouseCategoryName\":\"合格\",\"commodityId\":59743,\"commodityRemark\":\"\",\"deviceClassify\":\"体外诊断试剂\",\"warehouseAreaId\":90,\"logicAreaCode\":\"LC\",\"coldChainMark\":\"2153\",\"accountingOne\":\"\",\"classifyIdLevel1\":\"器械类-试剂\",\"classifyIdLevel2\":\"试剂-主试剂\",\"registerName\":\"缓冲液\",\"effectiveDate\":1662739200,\"commodityName\":\"待用---缓冲液1\"}],\"ip\":\"192.168.20.163\",\"timestamp\":1605058041041}"
      val rootNode = mapper.readTree(jsonString)
      System.out.println("businessName: " + rootNode.path("businessName").asText())
      System.out.println("companyName: " + rootNode.path("companyName").asText())
      System.out.println("timestamp: " + rootNode.path("timestamp").asLong())
      System.out.println("ip: " + rootNode.path("ip").asText())
      println("type:" + rootNode.path("dataList").getNodeType)
      println(rootNode.path("dataList").get(0).path("manufacturerName"))
      println(rootNode.path("dataList").get(1).path("manufacturerName"))
      val nodes = rootNode.findValues("dataList")
      val iterator = nodes.iterator
      while (iterator.hasNext){
        println(iterator.next().toString)
      }
    } catch {
      case e: JsonParseException =>
        e.printStackTrace
      case e: JsonMappingException =>
        e.printStackTrace
      case e: IOException =>
        e.printStackTrace
    }
  }

  def test1 : Unit = {
    import java.io.IOException
    try {
      val mapper = new ObjectMapper()
      val jsonString = "{\"name\":\"Mahesh Kumar\", \"age\":21,\"verified\":false,\"marks\": [100,90,85]}"
      val rootNode = mapper.readTree(jsonString)
      val nameNode = rootNode.path("name")
      System.out.println("Name: " + nameNode.asText())
      val ageNode = rootNode.path("age")
      System.out.println("Age: " + ageNode.asInt())
      val verifiedNode = rootNode.path("verified")
      System.out.println("Verified: " + (if (verifiedNode.asBoolean()) "Yes"
      else "No"))
      val marksJsonNode: JsonNode = rootNode.path("marks")
      val iterator = marksJsonNode.iterator()
      System.out.print("Marks: [ ")
      while ( {
        iterator.hasNext
      }) {
        val marks = iterator.next
        System.out.print(marks.asInt() + " ")
      }
      System.out.println("]")
    } catch {
      case e: JsonParseException =>
        e.printStackTrace()
      case e: JsonMappingException =>
        e.printStackTrace
      case e: IOException =>
        e.printStackTrace()
    }


  }

}
