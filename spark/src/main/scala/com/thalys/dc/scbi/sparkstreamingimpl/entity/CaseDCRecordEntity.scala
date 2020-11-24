package com.thalys.dc.scbi.sparkstreamingimpl.entity

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 09 15:24
  */
case class CaseDCRecordEntity (ip: String,
                   timestamp: Long,
                   companyName: String,
                   businessName: String,
                   dataList: Array[CaseStockEntity])
