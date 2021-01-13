package com.rex.demo.study.demo.driver.stream.join;

import com.rex.demo.study.demo.util.CommonUtils;
import com.rex.demo.study.demo.util.FlinkUtils2;
import org.apache.flink.api.common.functions.JoinFunction;
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

/**
 * TODO InnerJoin 实例
 *
 * @author liuzebiao
 * @Date 2020-2-21 16:11
 */
public class InnerJoinDemo {

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

        //实现 Join 操作(where、equalTo、apply 中实现部分，都可以写成单独一个类)
        DataStream<Tuple5<String, String, String, String, String>> joinDataStream = leftStream
                .join(rightStream)
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
                .apply(new JoinFunction<String, String, Tuple5<String, String, String, String, String>>() {
                    @Override
                    public Tuple5<String, String, String, String, String> join(String leftStr, String rightStr) throws Exception {
                        String[] left = leftStr.split(",");
                        String[] right = rightStr.split(",");
                        return new Tuple5<>(left[0], left[1], right[1], left[2],right[2]);
                    }
                });

        joinDataStream.print();

        env.execute("InnerJoinDemo");
    }
}

