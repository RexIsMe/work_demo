package com.rex.demo.study.demo.driver;

/**
 * @Author li zhiqang
 * @create 2020/12/4
 */

import com.alibaba.fastjson.JSONObject;
import com.rex.demo.study.demo.util.DateUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.Properties;


/**
 * 主要是event time、watermark的知识
 */
public class EventTimeDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(6);
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "1test_34fldink182ddddd344356");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        SingleOutputStreamOperator<JSONObject> kafkaSource = env.addSource(new FlinkKafkaConsumer<>("metric-topic", new SimpleStringSchema(), properties)).map(JSONObject::parseObject);

        kafkaSource
                .assignTimestampsAndWatermarks(WatermarkStrategy
                                .<JSONObject>forBoundedOutOfOrderness(Duration.ofSeconds(5))//水印策略
                                .withTimestampAssigner((record, ts) -> {
                                    DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//                            LocalDateTime parse = LocalDateTime.parse(record.getString("@timestamp"), pattern).plusHours(8);
//                            return parse.toInstant(ZoneOffset.of("+8")).toEpochMilli();
                                    return DateUtils.parseStringToLong(record.getString("@timestamp"),pattern,8, ChronoUnit.HOURS);
                                })//解析事件时间
                                .withIdleness(Duration.ofMinutes(1))//对于很久不来的流（空闲流，即可能一段时间内某源没有流来数据）如何处置
                )
                .keyBy(new KeySelector<JSONObject, String>() {
                    @Override
                    public String getKey(JSONObject record){
                        if (record.containsKey("process") && record.getJSONObject("process").containsKey("name")){
                            return record.getJSONObject("process").getString("name");
                        }else {
                            return "unknown-process";
                        }
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                //四个泛型分别是输入类型，输出类型，key和TimeWindow,这个process函数处理的数据是这个5s窗口中的所有数据
                .process(new ProcessWindowFunction<JSONObject, Tuple2<String,Long>, String, TimeWindow>() {
                    @Override
                    public void process(String key, Context context, Iterable<JSONObject> iterable, Collector<Tuple2<String,Long>> collector) throws Exception {
                        String time = null;
                        Long ts = 0L;
                        Iterator<JSONObject> iterator = iterable.iterator();
                        if (iterator.hasNext()){
                            JSONObject next = iterator.next();
                            time = next.getString("@timestamp");
                            DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
//                            time = LocalDateTime.parse(time, pattern).plusHours(8).toString().replace("T"," ");
                            ts = DateUtils.parseStringToLong(time, pattern, 8, ChronoUnit.HOURS);
                        }
                        collector.collect(new Tuple2<>(key,ts));
                    }
                })
                .print();
//        kafkaSource.print();
        env.execute();
    }
}
