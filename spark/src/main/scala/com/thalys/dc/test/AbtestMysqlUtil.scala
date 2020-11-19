//package com.thalys.dc.test
//
//import java.sql.{Connection, ResultSet, Statement}
//import java.util
//
//import com.mchange.v2.c3p0.ComboPooledDataSource
//import org.slf4j.{Logger, LoggerFactory}
//import com.kuaikan.bigdata.abtest.etl.config.ResourceConfig.mysqlConfig
//import com.typesafe.config.Config
//import org.joda.time.DateTime
//
//import scala.collection.mutable.ListBuffer
//
///**
//  * @author CJF
//  *    数据库连接池
//  *
//  * @date 2020-08-05
//  * @param mysqlKey
//  */
//abstract class MysqlUtil(mysqlKey: String) {
//
//  val logger: Logger = LoggerFactory.getLogger(classOf[MysqlUtil])
//  @transient val cpds: ComboPooledDataSource = new ComboPooledDataSource(true)
//
//  try {
//    val config: Config = mysqlConfig.getConfig(mysqlKey)
//    cpds.setJdbcUrl(config.getString("url"))
//    cpds.setUser(config.getString("user"))
//    cpds.setPassword(config.getString("pwd"))
//    cpds.setDriverClass("com.mysql.jdbc.Driver")
//    cpds.setMaxPoolSize(1)
//  } catch {
//    case e: Exception => e.printStackTrace()
//  }
//
//  def getConnection: Connection = {
//    cpds.getConnection()
//  }
//
//  def executeQuery(fun: Statement => Unit): Unit = {
//    val connection = getConnection
//    val statement = connection.createStatement()
//    try {
//      fun(statement)
//    } finally {
//      connection.close()
//    }
//  }
//}
//
//object AbtestMysqlUtil extends MysqlUtil("Abtest") {
//
//  /**
//    * 获取实验流量为0或者某个实验组流量为100,且更新时间在一个月之前的所有实验
//    *
//    * @return
//    */
//  def getExpireExperimentList(): List[String] = {
//
//    val preDay = DateUtil.getSpecificDate(DateTime.now().toString("yyyyMMdd"), -30)
//    val preDayWithBar = DateUtil.formatWithBars(preDay)
//
//    val sql =
//      s"""
//         |select distinct T1.experiment_id from
//         |(select * from experiment) T1
//         |join
//         |(select * from experiment_group) T2
//         |on T1.id=T2.experiment_id
//         |where (T1.flow_ratio=0 or T2.flow_ratio=100)
//         |and T1.experiment_status=1
//         |and T1.create_time>='2019-04-01'
//         |and T1.update_time<='${preDayWithBar}'
//      """.stripMargin
//
//    println(sql)
//    val result = ListBuffer[String]()
//
//    executeQuery(statement => {
//      val set: ResultSet = statement.executeQuery(sql)
//      while (set.next()) {
//        result += (set.getString("experiment_id"))
//      }
//    })
//    result.toList
//  }
//
//
//  def getExpireList(): util.List[String] = {
//    val list: List[String] = getExpireExperimentList()
//    import scala.collection.JavaConverters._
//    val result: util.List[String] = list.asJava
//    result
//  }
//
//
//}
