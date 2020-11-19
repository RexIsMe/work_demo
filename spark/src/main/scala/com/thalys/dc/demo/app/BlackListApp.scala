package com.thalys.dc.demo.app

import com.thalys.dc.demo.entity.AdsInfo
import com.thalys.dc.demo.util.RedisUtils
import org.apache.spark.sql._
import org.apache.spark.sql.streaming.Trigger
import redis.clients.jedis.Jedis
/**
  * descriptions:
  * 实现实时的动态黑名单检测机制：将每天对某个广告点击超过阈值(比如:100次)的用户拉入黑名单。
  * 1. 黑名单应该是每天更新一次. 如果昨天进入黑名单, 今天应该重新再统计
  * 2. 把黑名单写入到 redis 中, 以供其他应用查看
  * 3. 已经进入黑名单的用户不再进行检测(提高效率)
  *
  * 思路:
  * 写入到黑名单
  * 黑名单存放在 redis 中, 使用 set, set 中的每个元素表示一个用户.
  * 通过 sql 查询过滤出来每天每广告点击数超过阈值的用户, 然后使用 foreach 写入到 redis 即可.
  *
  * 过滤黑名单的用户点击记录
  * 先从 redis 读取到所有黑名单数据, 然后过滤, 只保留非黑名单用户的点击记录.
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 09 15:35
  */
object BlackListApp {

  def statBlackList(spark: SparkSession, adsInfoDS: Dataset[AdsInfo]): Dataset[AdsInfo] = {
    import spark.implicits._

    // 1. 过滤黑名单的数据: 如果有用户已经进入黑名单, 则不再统计这个用户的广告点击记录
    val filteredAdsInfoDS: Dataset[AdsInfo] = adsInfoDS.mapPartitions(adsInfoIt => { // 每个分区连接一次到redis读取黑名单, 然后把进入黑名单用户点击记录过滤掉
      val adsInfoList: List[AdsInfo] = adsInfoIt.toList
      if (adsInfoList.isEmpty) {
        adsInfoList.toIterator
      } else {
        // 1. 先读取到黑名单
        val client: Jedis = RedisUtils.getJedisClient

        val blackList: java.util.Set[String] = client.smembers(s"day:blcklist:${adsInfoList(0).dayString}")
        // 2. 过滤
        adsInfoList.filter(adsInfo => {
          !blackList.contains(adsInfo.userId)
        }).toIterator
      }

    })

    // 创建临时表: tb_ads_info
    filteredAdsInfoDS.createOrReplaceTempView("tb_ads_info")

    // 需求
    // 1: 黑名单 每天每用户每广告的点击量
    // 2.  按照每天每用户每id分组, 然后计数, 计数超过阈值(100)的查询出来
    val result: DataFrame = spark.sql(
      """
        |select
        | dayString,
        | userId
        |from tb_ads_info
        |group by dayString, userId, adsId
        |having count(1) >= 100000
      """.stripMargin)

    // 3. 把点击量超过 100 的写入到redis中.
    result.writeStream
      .outputMode("update")
      .trigger(Trigger.ProcessingTime("10 seconds"))
      .foreach(new ForeachWriter[Row] {
        var client: Jedis = _

        override def open(partitionId: Long, epochId: Long): Boolean = {
          // 打开到redis的连接
          client = RedisUtils.getJedisClient
          client != null
        }

        override def process(value: Row): Unit = {
          // 写入到redis  把每天的黑名单写入到set中  key: "day:blacklist" value: 黑名单用户
          val dayString: String = value.getString(0)
          val userId: String = value.getString(1)
          client.sadd(s"day:blcklist:$dayString", userId)
        }

        override def close(errorOrNull: Throwable): Unit = {
          // 关闭到redis的连接
          if (client != null) client.close()
        }
      })
      .option("checkpointLocation", "/Users/Administrator/Desktop/bigData/data/checkpoint/blacklist")
      .start()

    // 4. 把过滤后的数据返回   (在其他地方也可以使用临时表: tb_ads_info)
    filteredAdsInfoDS
  }

}
