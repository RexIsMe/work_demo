package com.thalys.dc.sparkstreaming.transform.withstate.window

/**
  * descriptions:
  * 1. reduceByKeyAndWindow(reduceFunc: (V, V) => V, windowDuration: Duration)
  * val wordAndOne: DStream[(String, Int)] = words.map((_, 1))
  * /*
  * 参数1: reduce 计算规则
  * 参数2: 窗口长度
  * 参数3: 窗口滑动步长. 每隔这么长时间计算一次.
  **/
  * val count: DStream[(String, Int)] =
  * wordAndOne.reduceByKeyAndWindow((x: Int, y: Int) => x + y,Seconds(15), Seconds(10))
  * 2. reduceByKeyAndWindow(reduceFunc: (V, V) => V, invReduceFunc: (V, V) => V, windowDuration: Duration, slideDuration: Duration)
  * 比没有invReduceFunc高效. 会利用旧值来进行计算.
  * invReduceFunc: (V, V) => V 窗口移动了, 上一个窗口和新的窗口会有重叠部分, 重叠部分的值可以不用重复计算了. 第一个参数就是新的值, 第二个参数是旧的值.
  * ssc.sparkContext.setCheckpointDir("hdfs://hadoop201:9000/checkpoint")
  * val count: DStream[(String, Int)] =
  *     wordAndOne.reduceByKeyAndWindow((x: Int, y: Int) => x + y,(x: Int, y: Int) => x - y,Seconds(15), Seconds(10))
  * 3. window(windowLength, slideInterval)
  * 基于对源 DStream 窗化的批次进行计算返回一个新的 Dstream
  * 4. countByWindow(windowLength, slideInterval)
  * 返回一个滑动窗口计数流中的元素的个数。
  * 5. countByValueAndWindow(windowLength, slideInterval, [numTasks])
  * 对(K,V)对的DStream调用，返回(K,Long)对的新DStream，其中每个key的的对象的v是其在滑动窗口中频率。如上，可配置reduce任务数量。
  *
  * author: li zhiqiang
  * date: 2020 - 11 - 16 14:19
  */
object WindowTest {

}
