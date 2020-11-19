package com.thalys.dc.scbi.structruestreamingimpl

import org.apache.spark.sql.SparkSession

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 14:25
  */
package object app {

//  @transient lazy val spark: SparkSession = SparkSession
//    .builder()
//    .appName("test")
//    .master("local[*]")
//    .getOrCreate()


  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .appName(this.getClass.getName)
    // 对sparks SQL专用的设置，并行度，big(所有spark executor 的核数，2）
    .config("spark.sql.shuffle.partitions", "8")
    // 只有在处理RDD时才会起作用，对Spark SQL的无效
    .config("spark.default.parallelism", "8")
    .config("spark.network.timeout", "200")
    .config("spark.sql.caseSensitive", "true")
    .config("spark.yarn.executor.memoryOverhead", "4096")
    .config("spark.yarn.driver.memoryOverhead", "4096")
    .config("spark.dynamicAllocation.enabled", "false")
    .config("spark.sql.session.timeZone", "UTC")
    //    .enableHiveSupport()
    .getOrCreate()



}
