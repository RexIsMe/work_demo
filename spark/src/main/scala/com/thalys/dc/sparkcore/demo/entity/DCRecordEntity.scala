package com.thalys.dc.sparkcore.demo.entity

import com.alibaba.fastjson.JSONObject
import java.util

/**
  * descriptions:
  * DC 消息记录实体类
  *
  * author: li zhiqiang
  * date: 2020 - 12 - 21 14:51
  */
case class DCRecordEntity(ip:String, timestamp:Long, companyName:String, businessName:String, dataList:util.List[JSONObject])
