package com.thalys.dc.sparkstreaming.source.kafka

import kafka.common.TopicAndPartition
import kafka.message.MessageAndMetadata
import kafka.serializer.StringDecoder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.kafka.KafkaCluster.Err
import org.apache.spark.streaming.kafka.{HasOffsetRanges, KafkaCluster, KafkaUtils, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 13:41
  */
object LowKafka {
  // 获取 offset
  def getOffset(kafkaCluster: KafkaCluster, group: String, topic: String): Map[TopicAndPartition, Long] = {
    // 最终要返回的 Map
    var topicAndPartition2Long: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()

    // 根据指定的主体获取分区信息
    val topicMetadataEither: Either[Err, Set[TopicAndPartition]] = kafkaCluster.getPartitions(Set(topic))
    // 判断分区是否存在
    if (topicMetadataEither.isRight) {
      // 不为空, 则取出分区信息
      val topicAndPartitions: Set[TopicAndPartition] = topicMetadataEither.right.get
      // 获取消费消费数据的进度
      val topicAndPartition2LongEither: Either[Err, Map[TopicAndPartition, Long]] =
        kafkaCluster.getConsumerOffsets(group, topicAndPartitions)
      // 如果没有消费进度, 表示第一次消费
      if (topicAndPartition2LongEither.isLeft) {
        // 遍历每个分区, 都从 0 开始消费
        topicAndPartitions.foreach {
          topicAndPartition => topicAndPartition2Long = topicAndPartition2Long + (topicAndPartition -> 0)
        }
      } else { // 如果分区有消费进度
        // 取出消费进度
        val current: Map[TopicAndPartition, Long] = topicAndPartition2LongEither.right.get
        topicAndPartition2Long ++= current
      }
    }
    // 返回分区的消费进度
    topicAndPartition2Long
  }

  // 保存消费信息
  def saveOffset(kafkaCluster: KafkaCluster, group: String, dStream: InputDStream[String]) = {

    dStream.foreachRDD(rdd => {
      var map: Map[TopicAndPartition, Long] = Map[TopicAndPartition, Long]()
      // 把 RDD 转换成HasOffsetRanges对
      val hasOffsetRangs: HasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
      // 得到 offsetRangs
      val ranges: Array[OffsetRange] = hasOffsetRangs.offsetRanges
      ranges.foreach(range => {
        // 每个分区的最新的 offset
        map += range.topicAndPartition() -> range.untilOffset
      })
      kafkaCluster.setConsumerOffsets(group,map)
    })
  }

  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("HighKafka")
    val ssc = new StreamingContext(conf, Seconds(3))
    // kafka 参数
    //kafka参数声明
    val brokers = "hadoop201:9092,hadoop202:9092,hadoop203:9092"
    val topic = "first"
    val group = "bigdata"
    val deserialization = "org.apache.kafka.common.serialization.StringDeserializer"
    val kafkaParams = Map(
      "zookeeper.connect" -> "hadoop201:2181,hadoop202:2181,hadoop203:2181",
      ConsumerConfig.GROUP_ID_CONFIG -> group,
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> brokers,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> deserialization,
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> deserialization
    )
    // 读取 offset
    val kafkaCluster = new KafkaCluster(kafkaParams)
    val fromOffset: Map[TopicAndPartition, Long] = getOffset(kafkaCluster, group, topic)
    val dStream = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, String](
      ssc,
      kafkaParams,
      fromOffset,
      (message: MessageAndMetadata[String, String]) => message.message()
    )
    dStream.print()
    // 保存 offset
    saveOffset(kafkaCluster, group, dStream)
    ssc.start()
    ssc.awaitTermination()
  }
}
