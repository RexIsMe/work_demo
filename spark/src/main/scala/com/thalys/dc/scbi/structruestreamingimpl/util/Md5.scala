package com.thalys.dc.scbi.structruestreamingimpl.util

import java.security.MessageDigest

/**
  * descriptions:
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 12 10:49
  */
object Md5 {
  def hashMD5(content: String): String = {
    val md5 = MessageDigest.getInstance("MD5")
    val encoded = md5.digest((content).getBytes)
    encoded.map("%02x".format(_)).mkString
  }

  def main(args: Array[String]) {
    println(hashMD5("abcdefg"))
    println(hashMD5("abcde"))
  }
}
