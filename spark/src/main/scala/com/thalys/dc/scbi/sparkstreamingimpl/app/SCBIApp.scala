package com.thalys.dc.scbi.sparkstreamingimpl.app

import com.thalys.dc.scbi.sparkstreamingimpl.entity.DCRecordEntity
import com.thalys.dc.scbi.sparkstreamingimpl.util.JacksonUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.kafka.KafkaUtils
import org.json4s.jackson.Json

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 14:53
  */
object SCBIApp {
  def createSSC(): StreamingContext = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))
    //每次最大消费10000记录
    conf.set("spark.streaming.kafka.maxRatePerPartition", "10000");
    //开启背压机制（根据处理效率调整 处理每批次 的记录数）
    conf.set("spark.streaming.backpressure.enabled", "true");
    // 偏移量保存在 checkpoint 中, 可以从上次的位置接着消费
    ssc.checkpoint("hdfs://hadoop-master:9000/spark/checkpoint/ck1")
    // kafka 参数
    //kafka参数声明
    val brokers = "172.26.55.116:9092,172.26.55.109:9092,172.26.55.117:9092"
    val topic = "test"
    val group = "bigdata"

    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      "zookeeper.connect" -> "172.26.55.106:2181,172.26.55.109:2181,172.26.55.115:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    val dStream: DStream[String] = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, Set(topic))
      .map(_._2)

    val recordDS: DStream[DCRecordEntity] = dStream.map(x => {
      JacksonUtils.readValue(x, classOf[DCRecordEntity])
    })

//    recordDS.print
    GetBatchNoApp.statBlackList(recordDS)

    ssc
  }

  def main(args: Array[String]): Unit = {
    // 设置将来访问 hdfs 的使用的用户名, 否则会出现权限不够
    System.setProperty("HADOOP_USER_NAME", "hadoop")
    val ssc: StreamingContext = StreamingContext.getActiveOrCreate("hdfs://hadoop-master:9000/spark/checkpoint/ck1", () => createSSC())
    ssc.start()
    ssc.awaitTermination()
  }
}
