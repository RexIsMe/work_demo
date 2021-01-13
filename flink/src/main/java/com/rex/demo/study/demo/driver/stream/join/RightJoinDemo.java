package com.rex.demo.study.demo.driver.stream.join;

import com.rex.demo.study.demo.util.CommonUtils;
import com.rex.demo.study.demo.util.FlinkUtils2;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * TODO coGroup()方法，实现 Left Join 功能
 *
 * @author liuzebiao
 * @Date 2020-2-21 16:11
 */
public class RightJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils2.getEnv();
        //设置使用 EventTime 作为时间标准
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //设置并行度为1,此处仅用作测试使用。因为 Kafka 为并行数据流，数据只有全部填满分区才会触发窗口操作。
        env.setParallelism(1);

        ParameterTool parameters1 = ParameterTool.fromPropertiesFile(CommonUtils.getResourcePath("config.properties"));
        ParameterTool parameters2 = ParameterTool.fromPropertiesFile(CommonUtils.getResourcePath("config2.properties"));

        DataStream<String> leftSource = FlinkUtils2.createKafkaStream(parameters1, SimpleStringSchema.class);
        DataStream<String> rightSource = FlinkUtils2.createKafkaStream(parameters2, SimpleStringSchema.class);

        //左流,设置水位线 WaterMark
        SingleOutputStreamOperator<String> leftStream = leftSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[2]);
            }
        });

        SingleOutputStreamOperator<String> rightStream = rightSource.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.milliseconds(0)) {
            @Override
            public long extractTimestamp(String s) {
                String[] split = s.split(",");
                return Long.parseLong(split[2]);
            }
        });

        // coGroup() 方法实现 right join 操作
        DataStream<Tuple5<String, String, String, String, String>> joinDataStream = rightStream
                .coGroup(leftStream)
                .where(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        String[] split = value.split(",");
                        return split[0];
                    }
                })
                .equalTo(new KeySelector<String, String>() {
                    @Override
                    public String getKey(String value) throws Exception {
                        String[] split = value.split(",");
                        return split[0];
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new CoGroupFunction<String, String, Tuple5<String, String, String, String, String>>() {
                    //重写 coGroup() 方法，来实现 left join 功能。
                    @Override
                    public void coGroup(Iterable<String> rightElement, Iterable<String> leftElement, Collector<Tuple5<String, String, String, String, String>> out) throws Exception {
                        boolean hasElement = false;
                        //leftElement为左流中的数据
                        for (String rightStr : rightElement) {
                            String[] right = rightStr.split(",");
                            //如果 左边的流 join 上右边的流,rightStream 就不能为空
                            for (String leftStr : leftElement) {
                                String[] left = leftStr.split(",");
                                //将 join 的数据输出
                                out.collect(Tuple5.of(right[0], right[1], left[1], right[2], left[2]));
                                hasElement = true;
                            }
                            if (!hasElement) {
                                out.collect(Tuple5.of(right[0], right[1], "null", right[2], "null"));
                            }
                        }
                    }
                });

        joinDataStream.print();

        env.execute("LeftJoinDemo");
    }
}

