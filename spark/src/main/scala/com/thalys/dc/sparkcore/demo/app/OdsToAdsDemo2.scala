package com.thalys.dc.sparkcore.demo.app

import java.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.thalys.dc.scbi.structruestreamingimpl.entity.StockEntity
import com.thalys.dc.sparkcore.demo.entity.{DCRecordEntity, Stock}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer

/**
  * descriptions:
  * ods到ads数据处理示例
  * 使用fastjson做json字符串与对象之间的转换
  *
  * author: li zhiqiang
  * date: 2020 - 12 - 21 13:28
  */
object OdsToAdsDemo2 {

  def main(args: Array[String]): Unit = {

    // TODO 1. 创建Spark配置对象
    val sparkConf = new SparkConf().setAppName("odsToAdsDemo").setMaster("local[*]")

    // TODO 2. 创建Spark环境连接对象
    // 创建Spark上下文环境对象的类，称之为Driver类
    // Spark-shell就是Driver
    val sc = new SparkContext(sparkConf)

    // TODO 3. 读取文件:
    //  3.1)从classpath中获取
    // Thread.currentThread().getContextClassLoader().getResourceAsStream()
    //  3.2) 从项目的环境中获取
    val lineRDD: RDD[String] = sc.textFile("hdfs://hadoop-master:9000/origin_data/dc/log/2020-12-21", 1)

    // TODO 4. 将每一行的json字符串转换成DCRecordEntity对象
    val value1: RDD[DCRecordEntity] = lineRDD.map(line => {
      JSON.parseObject(line, classOf[DCRecordEntity])
    })
//    value1.foreach(println)

    // TODO 5.1 过滤出stock的业务数据并将数据集压平
    val jsonObjRDD: RDD[JSONObject] = value1.filter(_.businessName.equals("stock"))
      .flatMap(json => {
        println("***解析数据***")
        json.dataList.toArray()
      })
    //TODO 5.2 这里特别注意：JSONArray的解析解析分两步
      .map(_.asInstanceOf[JSONObject])
    jsonObjRDD.foreach(println)

    //TODO 6. 将具体的数据元素转换成StockEntity对象
    val stockEntityRDD: RDD[StockEntity] =
      jsonObjRDD
      .map(ele => {
        //用这个方法会报错StockEntity$ cannot be cast to scala.runtime.Nothing$。
//        ele.toJavaObject(StockEntity.getClass)
        JSON.parseObject(ele.toJSONString,classOf[StockEntity])
      })

    //缓存中间RDD，避免重复计算
//    stockEntityRDD.cache()
    //直接打印查看转换结果
    stockEntityRDD.foreach(println)
//    stockEntityRDD.foreach(ele => {
//      println(ele.manufacturerName)
//      println(ele.customerEnableCode)
//    })

    //TODO 7. 具体业务逻辑
    val entity: StockEntity = stockEntityRDD.reduce(stockEntityReduceFunction)
    println(entity)



    // TODO 9. 释放连接
    sc.stop()

  }

  def stockEntityReduceFunction(se1:StockEntity, se2:StockEntity):StockEntity = {
    if(se1.equals(se2)){
      val sum: Int = se1.inventoryNumber + se2.inventoryNumber
      println(se1.inventoryNumber + ":" + se2.inventoryNumber)
      se1.inventoryNumber = sum
    }
    se1
  }

}
