package com.rex.demo.study.demo.driver.stream;

import com.rex.demo.study.demo.util.CommonUtils;
import com.rex.demo.study.demo.util.FlinkUtils;
import lombok.Data;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 热门商品 需求示例
 * 每隔5分钟统计出前面1小时内的热门商品topN：itemId、浏览量
 *
 * @Author li zhiqang
 * @create 2020/12/3
 */
public class HotItemDriver {

    public static void main(String[] args) throws Exception {
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        StreamExecutionEnvironment env = FlinkUtils.getEnv(FlinkUtils.FlinkStartConfig.builder()
                .flinkCheckpointConfig(FlinkUtils.FlinkCheckpointConfig.builder().build())
                .build());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        // 如果数据量比较大的时候，我们每条数据都生成一个水印的话，会影响性能，所以这里还有一个周期性生成水印的方法。这个水印的生成周期可以这样设置：
        env.getConfig().setAutoWatermarkInterval(5000L);

        env.readTextFile(CommonUtils.getResourcePath("UserBehavior.csv"))
        .map(line -> {
            String[] split = line.split(",");
            return new UserBehavior(Long.valueOf(split[0]), Long.valueOf(split[1]), Integer.valueOf(split[2]), split[3], Long.valueOf(split[4]));
        })
        /**
         * 指定eventTime字段、设置waterMark延时
         */
//        .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<UserBehavior>(Time.milliseconds(1)) {
//            @Override
//            public long extractTimestamp(UserBehavior element) {
//                return element.getTimestamp() * 1000;
//            }
//        })
//         .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessGenerator())
        /**
         * flink 1.11 timestampAsigner 和waterMark使用方式
         */
        .assignTimestampsAndWatermarks(WatermarkStrategy.<UserBehavior>forBoundedOutOfOrderness(Duration.ofMillis(10)).withTimestampAssigner(new SerializableTimestampAssigner<UserBehavior>(){
            @Override
            public long extractTimestamp(UserBehavior element, long recordTimestamp) {
                return element.getTimestamp() * 1000;
            }
        }))
        .filter(ub -> ub.getBehavior().equals("pv"))
        /**
         * lambda语法，:: 直接通过类型调用类的静态、非静态方法
         */
//        .keyBy(UserBehavior::getItemId)
        /**
         * 匿名类写法
         */
        .keyBy(new KeySelector<UserBehavior, Long>() {
            @Override
            public Long getKey(UserBehavior value) throws Exception {
                return value.getItemId();
            }
        })
        //上面的lambda写法
//        .keyBy(ub -> ub.getItemId())
        .timeWindow(Time.minutes(60),Time.minutes(5))
        .aggregate(new CountAgg(), new WindowResultFunction())
        .keyBy(ele -> ele.getWindowEnd())
        .process(new TopNHotItem())
        .printToErr();

        env.execute();
    }

}


class BoundedOutOfOrdernessGenerator implements AssignerWithPeriodicWatermarks<UserBehavior> {

    private final long maxOutOfOrderness = 3500L; // 3.5 seconds

    private long currentMaxTimestamp;

    @Override
    public long extractTimestamp(UserBehavior element, long previousElementTimestamp) {
        long timestamp = element.getTimestamp() * 1000;
        currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
        return timestamp;
    }

    @Override
    public Watermark getCurrentWatermark() {
        // return the watermark as current highest timestamp minus the out-of-orderness bound
        return new Watermark(currentMaxTimestamp - maxOutOfOrderness);
    }
}

/**
 * 键控流函数实现类
 */
class TopNHotItem extends KeyedProcessFunction<Long, ItemViewCount, String> {

//    private ListState<ItemViewCount> itemState = new ListStateDescriptor<ItemViewCount>();
    private ListState<ItemViewCount> itemState;

    /**
     * 创建一个ListState 用来存储数据
     * @param parameters
     * @throws Exception
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // 命名状态变量的名字和状态变量的类型
        ListStateDescriptor<ItemViewCount> itemViewCountListStateDescriptor = new ListStateDescriptor<>("itemState-state", ItemViewCount.class);
        // 从运行时上下文中获取状态并赋值
        itemState = getRuntimeContext().getListState(itemViewCountListStateDescriptor);
    }

    /**
     * 将每个元素都添加到ListState中
     * 注册一个定时器，出发时间设定为windowEnd + 1
     *
     * @param value
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void processElement(ItemViewCount value, Context ctx, Collector<String> out) throws Exception {
        itemState.add(value);
        ctx.timerService().registerEventTimeTimer(value.getWindowEnd() + 1);
    }

    /**
     * 定时器触发时，相当于收到了大于等于windowEnd + 1 的waterMark,可以认为这时窗口已经收到了该窗口的所有数据，从ListState中读取数据
     * @param timestamp
     * @param ctx
     * @param out
     * @throws Exception
     */
    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
        List<ItemViewCount> allItems = new ArrayList<>();
        Iterator<ItemViewCount> iterator = itemState.get().iterator();
        while (iterator.hasNext()) {
            allItems.add(iterator.next());
        }
        itemState.clear();

        Collections.sort(allItems);
//        List<ItemViewCount> sortedItems = allItems.subList(0, 3);
        List<ItemViewCount> sortedItems = allItems.stream().sorted().limit(3).collect(Collectors.toList());
        StringBuilder result = new StringBuilder();
        result.append("====================================\n");
        result.append("时间: ").append(new Timestamp(timestamp - 1)).append("\n");
        for (int i = 0; i < sortedItems.size(); i++) {
            ItemViewCount currentItem = sortedItems.get(i);
            // e.g.  No1：  商品ID=12224  浏览量=2413
            result.append("No").append(i+1).append(":")
                    .append("  商品ID=").append(currentItem.getItemId())
                    .append("  浏览量=").append(currentItem.getCount()).append("\n");
        }
        result.append("====================================\n\n");
        // 控制输出频率，模拟实时滚动结果
        Thread.sleep(1000);

        out.collect(result.toString());
    }

}


/**
 * 每个窗口内的每个键控流分别调用
 * 聚合逻辑
 * 计算浏览量
 */
class CountAgg implements AggregateFunction<UserBehavior, Long, Long> {

    @Override
    public Long createAccumulator() {
        return 0L;
}

    @Override
    public Long add(UserBehavior value, Long accumulator) {
        return accumulator + 1L;
    }

    @Override
    public Long getResult(Long accumulator) {
        return accumulator;
    }

    @Override
    public Long merge(Long a, Long b) {
        return a + b;
    }
}

/**
 *
 */
class WindowResultFunction implements WindowFunction<Long , ItemViewCount , Long, TimeWindow> {
    @Override
    public void apply(Long aLong, TimeWindow window, Iterable<Long> input, Collector<ItemViewCount> out) {
        out.collect(new ItemViewCount(aLong, window.getEnd(), input.iterator().next()));
    }
}


@Data
class UserBehavior {
    private Long userId;
    private Long itemId;
    private Integer categoryId;
    private String behavior;
    private Long timestamp;

    public UserBehavior(Long userId, Long itemId, Integer categoryId, String behavior, Long timestamp) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.timestamp = timestamp;
    }
}

@Data
class ItemViewCount implements Comparable<ItemViewCount> {
    private Long itemId;
    private Long windowEnd;
    private Long count;

    public ItemViewCount(Long itemId, Long windowEnd, Long count) {
        this.itemId = itemId;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    @Override
    public int compareTo(ItemViewCount o) {
        return o.getCount().compareTo(this.getCount());
    }
}
