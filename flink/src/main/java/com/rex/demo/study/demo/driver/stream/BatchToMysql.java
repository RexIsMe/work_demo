package com.rex.demo.study.demo.driver.stream;

import com.rex.demo.study.demo.sink.SinkToMySQL;
import com.rex.demo.study.demo.util.CommonUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 批量写入到mysql 示例
 *
 * @Author li zhiqang
 * @create 2020/11/25
 */
public class BatchToMysql {

    public static void main(String[] args) throws Exception {
        /**1.创建流运行环境**/
        StreamExecutionEnvironment env = CommonUtils.getEnv();

        /**2.Source:读取 Kafka 中的消息**/
        Properties properties = CommonUtils.getKafkaProperties();
        FlinkKafkaConsumer011<String> kafkaSource = new FlinkKafkaConsumer011<>("test", new SimpleStringSchema(), properties);
        DataStreamSource<String> stringDataStreamSource = env.addSource(kafkaSource);

        //3、流式数据每10s做为一个批次，写入到mysql
        SingleOutputStreamOperator<List<String>> streamOperator = stringDataStreamSource.timeWindowAll(Time.seconds(5)).apply(new AllWindowFunction<String, List<String>, TimeWindow>() {
            @Override
            public void apply(TimeWindow window, Iterable<String> values, Collector<List<String>> out) throws Exception {
                ArrayList<String> students = Lists.newArrayList(values);
                if (students.size() > 0) {
                    out.collect(students);
                }
            }
        });

        //4、每批的数据批量写入到mysql
        streamOperator.addSink(new SinkToMySQL());


        /**
         * 流式数据每3个元素做为一个批次，写入到mysql（不建议使用）
         */
//        stringDataStreamSource.countWindowAll(3).apply(new AllWindowFunction<String, List<String>, GlobalWindow>() {
//            @Override
//            public void apply(GlobalWindow window, Iterable<String> values, Collector<List<String>> out) throws Exception {
//                ArrayList<String> students = Lists.newArrayList(values);
//                if (students.size() > 0) {
//                    out.collect(students);
//                }
//            }
//        }).addSink(new SinkToMySQL());




        env.execute("metricsCounter");
    }

}
