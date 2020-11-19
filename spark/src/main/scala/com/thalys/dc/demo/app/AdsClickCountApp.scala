package com.thalys.dc.demo.app

import com.thalys.dc.demo.entity.AdsInfo
import com.thalys.dc.demo.util.RedisUtils
import org.apache.spark.sql.{Dataset, SparkSession}
import redis.clients.jedis.Jedis

/**
  * descriptions:
  * 每天每地区每城市广告点击量实时统计
  *
  * 思路：
  * 统计成功之后写入到redis
  * 值的类型使用hash
  * key                             value
  *
  * "date:area:city:ads"            field:                          value
  *                           2019-08-19:华北:北京:5                10000
  *
  * 使用sql 查询比较简单
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 09 15:38
  */
object AdsClickCountApp {
  val key: String = "date:area:city:ads"

  def statAdsClickCount(spark: SparkSession, filteredAdsInfoDS: Dataset[AdsInfo]) = {
    spark.sql(
      s"""
         |select
         |    dayString,
         |    area,
         |    city,
         |    adsId,
         |    count(1) count
         |from tb_ads_info
         |group by dayString, area, city, adsId
             """.stripMargin)
      .writeStream
      .outputMode("update")
      .foreachBatch((df, batchId) => { // 使用foreachBatch
        if (df.count() > 0) {
          df.cache() // 做缓存防止重复调用
          df.foreachPartition(rowIt => {
            val client: Jedis = RedisUtils.getJedisClient
            // 1. 把数据存入到map中, 向redis写入的时候比较方便
            val fieldValueMap: Map[String, String] = rowIt.map(row => {
              // 2019-08-19:华北:北京:5
              val field: String = s"${row.getString(0)}:${row.getString(1)}:${row.getString(2)}:${row.getString(3)}"
              val value: Long = row.getLong(4)
              (field, value.toString)
            }).toMap
            // 2. 写入到redis
            // 用于把scala的集合转换成java的集合
            import scala.collection.JavaConversions._
            if (fieldValueMap.nonEmpty) client.hmset(key, fieldValueMap)
            client.close()
          })

          df.unpersist() // 释放缓存
        }
      })
      .start
      .awaitTermination

  }
}
