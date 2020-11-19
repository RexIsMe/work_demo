package com.thalys.dc.scbi.sparkstreamingimpl

import org.apache.spark.sql.SparkSession

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 14:25
  */
package object app {

  @transient lazy val spark: SparkSession = SparkSession
    .builder()
    .appName("test")
    .master("local[*]")
    .getOrCreate()


//  @transient lazy val spark: SparkSession = SparkSession
//    .builder()
//    .appName(this.getClass.getName)
//    .config("spark.sql.shuffle.partitions", "4000")
//    .config("spark.default.parallelism", "8000")
//    .config("spark.network.timeout", "200")
//    .config("spark.sql.caseSensitive", "true")
//    .config("spark.yarn.executor.memoryOverhead", "4096")
//    .config("spark.yarn.driver.memoryOverhead", "4096")
//    .config("spark.dynamicAllocation.enabled", "false")
//    .config("spark.sql.session.timeZone", "UTC")
//    //    .enableHiveSupport()
//    .getOrCreate()



}
