package com.rex.demo.study.source

import java.util.Properties

import com.rex.demo.study.source
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.{TimeCharacteristic, scala}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
  * descriptions:
  * 获取flink已支持的数据源
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 23 9:27
  */
object GetExistSource {

  val BOOTSTRAP_SERVERS: String = "172.26.55.116:9092"

  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 设置流的时间为Processing Time
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)
    // 设置并行度为1，如果不设置，那么默认为当前机器的cpu的数量
    env.setParallelism(1)

//    var stream = getFromCollection(env)
//    var stream = getFromFile(env)
    var stream = getFromKafka(env)
//    var stream = getFromUDSource(env)

    stream.print()

    //开始执行
    env.execute()
  }



  /**
    * 示例
    * 从自定义数据源中获取DataStream
    *
    * @param env
    * @return
    */
  def getFromUDSource(env: StreamExecutionEnvironment): DataStream[source.SensorReading] = {
    // ingest sensor stream
    val sensorData: DataStream[source.SensorReading] = env
      // SensorSource generates random temperature readings
      .addSource(new SensorSource)
    sensorData
  }


  /**
    * 示例
    * 从文件读取数据
    *
    * @param env
    * @return
    */
  def getFromFile(env: StreamExecutionEnvironment): DataStream[String] = {
    env.readTextFile("")
  }


  /**
    * 示例
    * 以Kafka消息队列的数据为数据来源
    *
    * @param env
    * @return
    */
  def getFromKafka(env: StreamExecutionEnvironment): DataStream[String] ={
    val properties = new Properties()
    properties.setProperty("bootstrap.servers", BOOTSTRAP_SERVERS)
    properties.setProperty("group.id", "consumer-group")
    properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    properties.setProperty("auto.offset.reset", "latest")
    val stream = env
      // source为来自Kafka的数据，这里我们实例化一个消费者，topic为hotitems
      .addSource(new FlinkKafkaConsumer[String]("test", new SimpleStringSchema(), properties))

    stream
  }

  /**
    * 示例
    * 从批读取数据
    *
    * @param env
    * @return
    */
  def getFromCollection(env: StreamExecutionEnvironment): DataStream[SensorReading] = {
    val value: scala.DataStream[SensorReading] = env
      .fromCollection(List(
        SensorReading("sensor_1", 1547718199, 35.80018327300259),
        SensorReading("sensor_6", 1547718199, 15.402984393403084),
        SensorReading("sensor_7", 1547718199, 6.720945201171228),
        SensorReading("sensor_10", 1547718199, 38.101067604893444)
      ))
    value
  }

  // 传感器id，时间戳，温度
  case class SensorReading(
                            id: String,
                            timestamp: Long,
                            temperature: Double
                          )
}
