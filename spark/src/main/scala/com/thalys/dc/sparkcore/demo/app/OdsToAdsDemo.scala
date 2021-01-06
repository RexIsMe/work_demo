package com.thalys.dc.sparkcore.demo.app

import java.util

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.ObjectNode
import com.thalys.dc.scbi.structruestreamingimpl.entity.{DCRecordEntity, StockEntity}
import com.thalys.dc.sparkcore.demo.entity.Stock
import com.thalys.dc.util.JsonUtils
import javassist.bytecode.SignatureAttribute.ClassType
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.codehaus.jackson.`type`.TypeReference
import org.json4s.jackson.Json

import scala.collection.mutable.ArrayBuffer

/**
  * descriptions:
  * ods到ads数据处理示例
  * 使用jackson做json字符串与对象之间的转换
  *
  * author: li zhiqiang
  * date: 2020 - 12 - 21 13:28
  */
object OdsToAdsDemo {

  def main(args: Array[String]): Unit = {

    // 创建第一个Spark应用程序 : WordCount

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

    // TODO 4. 将每一行的字符串拆分成一个一个的单词(扁平化)
    val value1: RDD[JsonNode] = lineRDD.map(line => {
      JsonUtils.getRootNode(line)
    })
    value1.foreach(println)

    val value: RDD[StockEntity] =
      value1.filter(_.path("businessName").asText().equals("stock"))
      .flatMap(json => {
        println("***解析数据***")
        //        json.getJSONArray("dataList").toArray()
                val value: util.Iterator[JsonNode] = json.path("dataList").elements()
        val nodes = new ArrayBuffer[JsonNode]()
        while (value.hasNext) {
          nodes.+=(value.next())
        }
        nodes
      })
      .map(ele => {
        JsonUtils.toObject(ele.toPrettyString, classOf[StockEntity])
      })

//    value.cache()
    value.foreach(ele => println(ele.toString))
//    value.foreach(ele => {
//      println(ele.manufacturerName)
//      println(ele.customerEnableCode)
//    })

//    value.reduce((json1, json2) => {
//      if(json1.equals(json2)){
//
//      }
//      json1
//    })


    // TODO 9. 释放连接
    sc.stop()

  }

}
