package com.rex.demo.study.demo.driver

import java.sql.PreparedStatement

import com.rex.demo.study.demo.util.CommonUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.jdbc._
/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2021 - 02 - 20 14:43
  */
object SkinCH {
  //用户信息样例类
  case class User(id: Int, date: String, age:Int)
  //重写JdbcStatementBuilder
  class CHSinkBuilder extends JdbcStatementBuilder[User] {
    override def accept(t: PreparedStatement, u: User): Unit = {
      t.setInt(1, u.id)
      t.setString(2, u.date)
      t.setInt(3, u.age)
    }
  }
  def main(args: Array[String]): Unit = {
    // 1、获取流式环境变量
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // 2、从文件中读取数据
//    val filePath = "src/main/resources/user.csv"
    val dataStream = env.readTextFile(CommonUtils.getResourcePath("user.csv"))
      .map(line => {
        val arr = line.split(",")
        User(arr(0).toInt, "2021-02-20", arr(3).toInt)
      })
    val skinSql =
      """
        |INSERT INTO user(
        |id, date, age
        |) VALUES(
        |?,?,?
        |)
        |""".stripMargin
    // 3、 skin ClickHouse
    dataStream.addSink(
      JdbcSink.sink(
        skinSql,
        new CHSinkBuilder,
        new JdbcExecutionOptions.Builder()
          .withBatchSize(1)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUrl("jdbc:clickhouse://172.16.0.213:8123/test")
//          .withUsername("admin")
//          .withPassword("123456")
          .build()
      )
    )
    // 4、执行任务
    env.execute("SinkCH Job")
  }
}


/**
  *
  * // 建数据库
  * CREATE database test;
  * // 建MergeTree表
  * CREATE table test.user(
  * id Int32,
  * date Date,
  * age Int8
  * )
  * engine=MergeTree()
  * order by id
  * ;
  * // 测试插入
  * INSERT into test.user values (4, '2020-01-12', 19), (5, '2020-02-13', 3);
  * // 查看
  * SELECT * FROM test.user;
  *
  *
  */
