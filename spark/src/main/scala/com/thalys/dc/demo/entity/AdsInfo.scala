package com.thalys.dc.demo.entity

import java.sql.Timestamp

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 09 15:24
  */
case class AdsInfo(ts: Long,
                   timestamp: Timestamp,
                   dayString: String,
                   hmString: String,
                   area: String,
                   city: String,
                   userId: String,
                   adsId: String)
