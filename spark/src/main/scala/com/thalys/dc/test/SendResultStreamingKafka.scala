//package com.thalys.dc.test
//
//import java.nio.charset.Charset
//import java.util.Calendar
//
//import com.alibaba.fastjson.JSON
//import com.fasterxml.jackson.annotation.{JsonIgnoreProperties, JsonProperty}
//import com.fasterxml.jackson.databind.ObjectMapper
//import com.fasterxml.jackson.module.scala.DefaultScalaModule
//import com.kuaikan.data.push.util._
//import kafka.message.MessageAndMetadata
//import kafka.serializer.StringDecoder
//import org.apache.commons.codec.binary.Base64
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.{FileSystem, Path}
//import org.apache.spark.SparkConf
//import org.apache.spark.internal.Logging
//import org.apache.spark.rdd.RDD
//import org.apache.spark.sql.{SQLContext, SaveMode}
//import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka.{HasOffsetRanges, InnerSpark, KafkaUtils}
//import org.joda.time.DateTime
//import com.kuaikan.data.push.DB._
//import com.kuaikan.data.push.statistics._
//import org.apache.commons.lang3.StringUtils
//
//import scala.util.Try
//
///**
//  * descriptions:
//  * 小方的示例代码
//  *
//  * author: li zhiqiang
//  * date: 2020 - 11 - 16 14:36
//  */
//case class PushResultInfo(
//                           @JsonProperty("push_task_id") pushTaskId: Long,
//                           @JsonProperty("push_partner") pushPartner: Int,
//                           @JsonProperty("push_partner_name") pushPartnerName: String,
//                           @JsonProperty("push_task_subset_id") pushSubSetId: Long,
//                           @JsonProperty("devices") devices: List[String])
//
//@JsonIgnoreProperties(ignoreUnknown = true)
//case class PushHandleResultInfo(
//                                 @JsonProperty("pushTaskId") pushTaskId: Long,
//                                 @JsonProperty("partnerCode") pushPartner: Int,
//                                 @JsonProperty("msgSDK") pushPartnerName: String,
//                                 @JsonProperty("pushSubTaskId") pushSubSetId: Long,
//                                 @JsonProperty("device") device: String,
//                                 @JsonProperty("resultCode") resultCode: Int)
//
//case class ParquetSingleUserPushResult(taskId: Long,
//                                       pushPartner: Int,
//                                       pushPartnerName: String,
//                                       platform: Int,
//                                       pushSubSetId: Long,
//                                       deviceId: String,
//                                       event: PushResultEvent)
//
//object SendResultStreamingKafka extends PushLogging {
//  val pushAppName = "push_result"
//
//  def functionToCreateContext(): StreamingContext = {
//    val conf = new SparkConf().setAppName(pushAppName)
//    conf.set("spark.streaming.kafka.maxRatePerPartition", "10000");
//    conf.set("spark.streaming.backpressure.enabled", "true");
//    val ssc = new StreamingContext(conf, Minutes(5))
//    val sqlContext = new SQLContext(ssc.sparkContext)
//
//    logging(s"kafka broker $brokerList...")
//    val kafkaParams = Map[String, String](
//      "metadata.broker.list" -> brokerList,
//      "auto.offset.reset" -> "smallest"
//    )
//    val partition2Offset = OffsetUtils.getOffsetFromDB(pushAppName)
//    val sourceStream = if (partition2Offset.isEmpty) {
//      logging(s"$pushResultKafkaTopic don't have any offset save in mysql,so we consume from largest offset")
//      InnerSpark.createDirectStreamWithTopicName[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set(pushResultKafkaTopic, pushResultClickTopic, pushResultReceiveTopic, pushResultXiaFaTopic))
//    } else {
//
//      logging(s"consume $pushResultKafkaTopic from specified offset,$partition2Offset")
//      val messageHandler = (mmd: MessageAndMetadata[String, String]) => (mmd.topic, mmd.message())
//      KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder, (String, String)](ssc, kafkaParams, partition2Offset, messageHandler)
//    }
//    sourceStream.foreachRDD(rdd => {
//      if (!rdd.isEmpty()) {
//        val hasOffsetRanges = rdd.asInstanceOf[HasOffsetRanges]
//        OffsetUtils.writeOffset2DB(pushAppName, hasOffsetRanges)
//        import sqlContext.implicits._
//
//        val (pushSuccessRdd, pushResultActionRdd, xfRdd) = splitRdd(rdd)
//
//        val pushResultActionDF = pushResultActionRdd.map(kv => {
//          val (_, message) = kv
//          message
//        }).collect(new PushResultActionLogPartialFun()).flatMap(seq => seq).toDS()
//
//
//        //下发数据
//        val pushResultXiaFaDF = xfRdd.mapPartitions(iters => {
//          val mapper = new ObjectMapper()
//          mapper.registerModule(DefaultScalaModule)
//          iters.map(kv => {
//            val (_, value) = kv
//            val content: String = StringUtils.strip(value)
//            var info:PushHandleResultInfo = null
//            try {
//              info = mapper.readValue(content, classOf[PushHandleResultInfo])
//            } catch {
//              case e:Exception => println(e)
//            }
//            info
//          }).filter(x=>x!=null).filter(x => x.resultCode == 2)
//            .map(info => ParquetSingleUserPushResult(info.pushTaskId,
//              info.pushPartner,
//              info.pushPartnerName,
//              getPlatformIntNumber(info.device),
//              info.pushSubSetId,
//              info.device,
//              PushEventXiaFa))
//        }).toDS()
//
//
//        val pushSendSuccessDF = pushSuccessRdd.mapPartitions(iters => {
//          val mapper = new ObjectMapper()
//          mapper.registerModule(DefaultScalaModule)
//          iters.flatMap(kv => {
//            val (_, value) = kv
//            val info = mapper.readValue(value, classOf[PushResultInfo])
//            val singleUserPushResultList = info.devices.map(device => {
//              ParquetSingleUserPushResult(info.pushTaskId,
//                info.pushPartner,
//                info.pushPartnerName,
//                getPlatformIntNumber(device),
//                info.pushSubSetId,
//                device,
//                PushEventSend)
//            })
//            singleUserPushResultList
//          })
//        }).toDS()
//        val sourceRDD = pushResultActionDF.union(pushSendSuccessDF).union(pushResultXiaFaDF).rdd
//          .filter(pushResult => {
//            pushResult.taskId > 0
//          })
//
//        statsticDF2TaskRecord(sourceRDD)
//        statsticDF2Date(sourceRDD)
//        statstic2SubSet(sourceRDD)
//        stastic2TaskRelation(sourceRDD)
//      }
//    })
//    ssc
//  }
//
//  def statsticDF2TaskRecord(rdd: RDD[ParquetSingleUserPushResult]): Unit = {
//
//    val listHandle = rdd.map(push => {
//      (push.taskId, push.event, push.platform, push.pushPartner)
//    }).countByValue().map(kv => {
//      val ((taskId, event, platform, pushPartner), count) = kv
//      logging(s"update $mongoPushDetailStatisticsCol key :(taskId:$taskId,platform: $platform, pushPartner :$pushPartner) increase $count")
//      val handle = PushMongoHandle(event, count, mongoPushDetailStatisticsCol, platform, pushPartner, KeyAndValue("task_id", taskId))
//      handle
//    }).toList
//    BackMongoUtil.update2MongoMetricsList(listHandle, mongoPushDetailStatisticsCol)
//
//  }
//
//  def statsticDF2Date(rdd: RDD[ParquetSingleUserPushResult]): Unit = {
//
//
//    val listHandle = rdd.map(push => {
//      (push.event, push.platform, push.pushPartner)
//    }).countByValue().map(kv => {
//      val zeroTimeMills = CommonMethod.getZeroClockTimeStamp(Calendar.getInstance())
//      val ((event, platform, pushPartner), count) = kv
//      logging(s"update $mongoPushDateStatisticsCol key( event:$event,platform:$platform, pushPartner:$pushPartner) increase $count")
//      val handle = PushMongoHandle(event, count, mongoPushDateStatisticsCol, platform, pushPartner, KeyAndValue("push_at", zeroTimeMills))
//      handle
//    }).toList
//    BackMongoUtil.update2MongoMetricsList(listHandle, mongoPushDateStatisticsCol)
//
//  }
//
//  def statstic2SubSet(rdd: RDD[ParquetSingleUserPushResult]): Unit = {
//
//
//    val listHandle = rdd.filter(result => {
//      result.pushSubSetId > 0
//    }).map(push => {
//      (push.event, push.pushSubSetId, push.platform, push.pushPartner)
//    }).countByValue().filter(kv => kv._1._2 != 0).map(kv => {
//      val ((event, subsetId, platform, pushPartner), count) = kv
//      logging(s"update $mongoPushSubSetStatisticsCol key(subsetId:$subsetId,event: $event,platform:$platform, pushPartner:$pushPartner) increase $count")
//      val handle = PushMongoHandle(event, count, mongoPushSubSetStatisticsCol, platform, pushPartner, KeyAndValue("task_subset_id", subsetId))
//      handle
//    }).toList
//    BackMongoUtil.update2MongoMetricsList(listHandle, mongoPushSubSetStatisticsCol)
//  }
//
//  def stastic2TaskRelation(rdd: RDD[ParquetSingleUserPushResult]): Unit = {
//
//
//    val resultMap = rdd.filter(push => {
//      push.pushSubSetId > 0
//    }).map(push => {
//      (push.taskId, push.pushSubSetId, push.event, push.platform, push.pushPartner)
//    }).countByValue().filter(kv => kv._1._2 != 0)
//
//    val taskInfos = resultMap.map(kv => {
//      val ((taskId, subsetId, event, platform, pushPartner), count) = kv
//      (taskId, subsetId)
//    }).toList
//    BackMongoUtil.noExistsThenInsertV2List(taskInfos)
//    val listHandle = resultMap.map(kv => {
//      val ((taskId, subsetId, event, platform, pushPartner), count) = kv
//      logging(s"update $mongoPushTaskRelation key(event: $event,task_id:$taskId,subsetId:$subsetId,platform:$platform,pushPartner:$pushPartner) increase $count")
//      val handle = PushMongoHandle(event, count, mongoPushTaskRelation, platform, pushPartner, KeyAndValue("task_subset_id", subsetId), KeyAndValue("task_id", taskId))
//      handle
//    }).toList
//    BackMongoUtil.update2MongoMetricsList(listHandle, mongoPushTaskRelation)
//  }
//
//
//  def isShutdownRequested(): Boolean = {
//    val fs = FileSystem.get(new Configuration())
//    fs.exists(new Path(shutdownPath))
//  }
//
//  def splitRdd(rdd: RDD[(String, String)]) = {
//    val pushSuccessRdd = rdd.filter(kv => {
//      val (topic, _) = kv
//      topic == pushResultKafkaTopic
//    })
//    //到达和点击数据
//    val pushResultActionRdd = rdd.filter(kv => {
//      val (topic, _) = kv
//      topic == pushResultClickTopic || topic == pushResultReceiveTopic
//    })
//    //下发数据
//    val xfRdd = rdd.filter(kv => {
//      val (topic, _) = kv
//      topic == pushResultXiaFaTopic
//    })
//    (pushSuccessRdd, pushResultActionRdd, xfRdd)
//  }
//
//  def getPushEventByEventName(eventName: String) = {
//    eventName match {
//      case `pushResultClickTopic` => PushEventClick
//      case `pushResultReceiveTopic` => PushEventReceive
//    }
//  }
//
//  def getPlatformIntNumber(deviceId: String) = {
//    if (CommonMethod.isAndroidDevice(deviceId))
//      1
//    else
//      2
//
//  }
//
//
//  def main(args: Array[String]): Unit = {
//
//
//    val ssc = functionToCreateContext()
//    ssc.start()
//    val checkIntervalMillis = 1000
//    var isStopped = false
//    while (!isStopped) {
//      isStopped = ssc.awaitTerminationOrTimeout(checkIntervalMillis)
//      if (!isStopped && isShutdownRequested) {
//        val stopSparkContext = true
//        val stopGracefully = true
//        isStopped = true
//        logging("--------shutdown application------------")
//        ssc.stop(stopSparkContext, stopGracefully)
//      }
//    }
//  }
//}
//
//class PushResultActionLogPartialFun extends PartialFunction[String, Seq[ParquetSingleUserPushResult]] with Serializable {
//  lazy val base64 = new Base64()
//  var userPushInfo: Seq[ParquetSingleUserPushResult] = null
//
//  override def isDefinedAt(x: String): Boolean = {
//    userPushInfo = parsePushResultActionLog(x, base64)
//    userPushInfo.nonEmpty
//  }
//
//  override def apply(v1: String): Seq[ParquetSingleUserPushResult] = {
//    userPushInfo
//  }
//
//  def getPushParterIntValue(partnerName: String) = {
//    partnerName match {
//      case "小米通道" => 2
//      case "极光通道" => 1
//      case "个推通道" => 3
//      case "oppo通道" => 4
//      case _ => 30
//    }
//  }
//
//  def parsePushResultActionLog(line: String, base64: Base64): Seq[ParquetSingleUserPushResult] = {
//    try {
//      val linearr = line.split("\t")
//      val deviceId = linearr(2)
//      val regex = "\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2}  \\[.+\\] - \\[ INFO \\]  \\[(\\w+)\\](.+)".r
//      linearr(0) match {
//        case regex(eventName, encodedContent) =>
//          val jsonString = new String(base64.decode(encodedContent), Charset.forName("UTF-8"))
//          val nObject = JSON.parseObject(jsonString)
//          Try(CommonMethod.jsonArray2ArrayJson(nObject.getJSONArray("properties")))
//            .getOrElse(Seq(nObject.getJSONObject("properties"))).map(json => {
//            val pushId = if (json.containsKey("PushTaskID"))
//              json.getLongValue("PushTaskID")
//            else -1
//            val pushTaskSubsetID = if (json.containsKey("PushTaskSubsetID")) json.getLongValue("PushTaskSubsetID") else -1l
//            val parterName = json.getString("MsgSDK")
//            ParquetSingleUserPushResult(pushId,
//              getPushParterIntValue(parterName),
//              parterName,
//              SendResultStreamingKafka.getPlatformIntNumber(deviceId),
//              pushTaskSubsetID,
//              deviceId,
//              SendResultStreamingKafka.getPushEventByEventName(eventName))
//          }).filter(event => event.taskId != -1)
//
//      }
//    } catch {
//      case e: Exception =>
//        println(line)
//        Seq[ParquetSingleUserPushResult]()
//    }
//  }
//}
