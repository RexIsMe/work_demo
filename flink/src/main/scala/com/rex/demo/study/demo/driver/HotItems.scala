package com.rex.demo.study.demo.driver


import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object HotItems {
    def main(args: Array[String]): Unit = {

        val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
        env.setParallelism(1)

//        val properties = new Properties()
//        properties.setProperty("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
//        properties.setProperty("group.id", "consumer-group")
//        properties.setProperty("key.deserializer",
//            "org.apache.kafka.common.serialization.StringDeserializer")
//        properties.setProperty("value.deserializer",
//            "org.apache.kafka.common.serialization.StringDeserializer")
//        properties.setProperty("auto.offset.reset", "latest")
//        env.addSource(new FlinkKafkaConsumer[String]("hotitems", new SimpleStringSchema(), properties))

        env.readTextFile("C:\\workspace\\own\\大数据\\flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")
        //将获取的数据进行样例类包装
        .map(line=>{
            val arr: Array[String] = line.split(",")
            UserBehavior(arr(0).toLong,arr(1).toLong,arr(2).toInt,arr(3),arr(4).toLong)
        })
        .assignAscendingTimestamps(_.timestamp * 1000)
        .filter(_.behavior == "pv")
        .keyBy("itemId")
        .timeWindow(Time.minutes(60),Time.minutes(5))
        .aggregate(new CountAgg() , new WindowResultFunction())
        .keyBy(1)
        .process( new TopNHotItem(3))
        .print()

        env.execute("hot item job")

    }

  /**
      * 窗口聚合策略——每出现一条记录就加一
      */
    class CountAgg() extends AggregateFunction[UserBehavior,Long,Long] {
        override def createAccumulator(): Long = 0L

        override def add(in: UserBehavior, acc: Long): Long = acc + 1

        override def getResult(acc: Long): Long = acc

        override def merge(acc: Long, acc1: Long): Long = acc + acc1
    }

    /**
      * IN: 输入为累加器的类型，Long
      * OUT: 窗口累加以后输出的类型为 ItemViewCount(itemId: Long, windowEnd: Long, count: Long), windowEnd为窗口的结束 时间，也是窗口的唯一标识
      * KEY: Tuple泛型，在这里是 itemId，窗口根据itemId聚合
      * W: 聚合的窗口，w.getEnd 就能拿到窗口的结束时间
      */
    class WindowResultFunction extends WindowFunction[Long , ItemViewCount ,Tuple, TimeWindow] {
        override def apply(key: Tuple,
                           window: TimeWindow,
                           input: Iterable[Long],
                           out: Collector[ItemViewCount]): Unit = {
            val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
            val count: Long = input.iterator.next()
            out.collect(ItemViewCount (itemId , window.getEnd , count))
        }
    }

    /**
      * 键控流方法，得到topN
      * @param topSize
      */
    class TopNHotItem(topSize : Int) extends  KeyedProcessFunction[Tuple, ItemViewCount, String]{
        private var itemState : ListState[ItemViewCount] = _

        override def open(parameters: Configuration): Unit = {
            super.open(parameters)
            // 命名状态变量的名字和状态变量的类型
            val itemsStateDesc = new ListStateDescriptor[ItemViewCount]("itemState-state", classOf[ItemViewCount])
            // 从运行时上下文中获取状态并赋值
            itemState = getRuntimeContext.getListState(itemsStateDesc)
        }

        override def processElement(i: ItemViewCount,
                                    context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context,
                                    collector: Collector[String]): Unit = {
            itemState.add(i)
            context.timerService.registerEventTimeTimer(i.windowEnd + 1)
        }

        override def onTimer(timestamp: Long,
                             ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext,
                             out: Collector[String]): Unit = {
            val allItems: ListBuffer[ItemViewCount] = ListBuffer()
            import scala.collection.JavaConversions._
            for ( item <- itemState.get ) {
                allItems += item
            }
            itemState.clear()

            val sortedItems: ListBuffer[ItemViewCount] = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)
            val result: StringBuilder = new StringBuilder
            result.append("====================================\n")
            result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n")

            for(i <- sortedItems.indices){
                val currentItem: ItemViewCount = sortedItems(i)
                // e.g.  No1：  商品ID=12224  浏览量=2413
                result.append("No").append(i+1).append(":")
                        .append("  商品ID=").append(currentItem.itemId)
                        .append("  浏览量=").append(currentItem.count).append("\n")
            }
            result.append("====================================\n\n")
            // 控制输出频率，模拟实时滚动结果
            Thread.sleep(1000)
            out.collect(result.toString)

        }
    }
    case class UserBehavior(userId: Long,
                            itemId: Long,
                            categoryId: Int,
                            behavior: String,
                            timestamp: Long)
    case class ItemViewCount(itemId: Long,
                             windowEnd: Long,
                             count: Long)

}
