package com.thalys.dc.structruestreaming.sink.foreach

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, ForeachWriter, Row, SparkSession}

/**
  * foreach sink 会遍历表中的每一行, 允许将流查询结果按开发者指定的逻辑输出.
  * 把 wordcount 数据写入到 mysql
  *
  * 延迟有点高
  */
object ForeachSink {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("ForeachSink")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "172.26.55.109")
      .option("port", 10000)
      .load

    val wordCount: DataFrame = lines.as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value").count()

    val query: StreamingQuery = wordCount.writeStream
      .outputMode("update")
      // 使用 foreach 的时候, 需要传递ForeachWriter实例, 三个抽象方法需要实现. 每个批次的所有分区都会创建 ForeeachWriter 实例
      .foreach(new ForeachWriter[Row] {
        var conn: Connection = _
        var ps: PreparedStatement = _
        var batchCount = 0

        // 一般用于 打开链接. 返回 false 表示跳过该分区的数据,
        override def open(partitionId: Long, epochId: Long): Boolean = {
          println("open ..." + partitionId + "  " + epochId)
          Class.forName("com.mysql.jdbc.Driver")
          conn = DriverManager.getConnection("jdbc:mysql://172.26.55.109:3306/dc", "root", "t4*9&/y?c,h.e17!")
          // 插入数据, 当有重复的 key 的时候更新
          val sql = "insert into word_count values(?, ?) on duplicate key update word=?, count=?"
          ps = conn.prepareStatement(sql)

          conn != null && !conn.isClosed && ps != null
        }

        // 把数据写入到连接
        override def process(value: Row): Unit = {
          println("process ...." + value)
          val word: String = value.getString(0)
          val count: Long = value.getLong(1)
          ps.setString(1, word)
          ps.setLong(2, count)
          ps.setString(3, word)
          ps.setLong(4, count)
          ps.execute()
        }

        // 用户关闭连接
        override def close(errorOrNull: Throwable): Unit = {
          println("close...")
          ps.close()
          conn.close()
        }
      })
      .start

    query.awaitTermination()

  }

}
