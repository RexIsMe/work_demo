package com.rex.demo.study.demo.driver.stream;

import com.rex.demo.study.demo.sink.MyRedisSink;
import com.rex.demo.study.demo.util.CommonUtils;
import com.rex.demo.study.demo.util.FlinkUtils2;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.net.URL;
import java.util.Arrays;

/**
 1. TODO 实时读取 Kafka 单词，计算并保存数据到 Redis
 2.  3. @author liuzebiao
 3. @Date 2020-2-18 9:50
 */
public class MyRedisSinkTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = FlinkUtils2.getEnv();

//        ParameterTool parameters = ParameterTool.fromPropertiesFile("C:\\workspace\\own\\mygithub\\work_demo\\flink\\src\\main\\resources\\config.properties");
        URL resource = PropertiesConfiguration.PropertiesReader.class.getClassLoader().getResource("config.properties");
        ParameterTool parameters = ParameterTool.fromPropertiesFile(resource.getPath());

        DataStream<String> kafkaStream = FlinkUtils2.createKafkaStream(parameters, SimpleStringSchema.class);

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> tuple3Operator = kafkaStream.flatMap((String lines, Collector<Tuple3<String, String, Integer>> out) -> {
            Arrays.stream(lines.split(" ")).forEach(word -> out.collect(Tuple3.of("WordCount",word, 1)));
        }).returns(Types.TUPLE(Types.STRING, Types.STRING,Types.INT));

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> sum = tuple3Operator.keyBy(1).sum(2);

        sum.addSink(new MyRedisSink());

        env.execute("Test");
    }
}
