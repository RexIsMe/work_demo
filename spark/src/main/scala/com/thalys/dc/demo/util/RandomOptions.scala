package com.thalys.dc.demo.util

import scala.collection.mutable.ListBuffer
/**
  *
  */
object RandomOptions {

  def apply[T](opts: (T, Int)*): RandomOptions[T] = {
    val randomOptions = new RandomOptions[T]()
    randomOptions.totalWeight = (0 /: opts) (_ + _._2) // 计算出来总的比重
    opts.foreach {
      case (value, weight) => randomOptions.options ++= (1 to weight).map(_ => value)
    }
    randomOptions
  }


  def main(args: Array[String]): Unit = {
    // 测试
    val opts = RandomOptions(("张三", 10), ("李四", 30), ("ww", 20))

    println(opts.getRandomOption())
    println(opts.getRandomOption())
  }
}

// 工程师 10  程序猿 10  老师 20
class RandomOptions[T] {
  var totalWeight: Int = _
  var options = ListBuffer[T]()
  /**
    * 获取随机的 Option 的值
    *
    * @return
    */
  def getRandomOption() = {
    options(RandomNumUtils.randomInt(0, totalWeight - 1))
  }

}
