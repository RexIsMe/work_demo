package com.rex.demo.study.demo.sink;

import com.rex.demo.study.demo.constants.KafkaConstants;
import com.rex.demo.study.demo.entity.FlinkInitInfo;
import com.rex.demo.study.demo.enums.KafkaConfigEnum;
import com.rex.demo.study.demo.util.FlinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

import java.util.Properties;

/**
 * kafka sink 示例
 * 使用官方提供的kafka sink
 *
 * @Author li zhiqang
 * @create 2020/12/1
 */
@Slf4j
public class MyKafkaSink {

    public static void main(String[] args) throws Exception {
        // 设置将来访问 hdfs 的使用的用户名, 否则会出现权限不够
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        FlinkInitInfo flinkKafkaInitInfo = FlinkUtils.getFlinkKafkaInitInfo(FlinkUtils.FlinkStartConfig.builder()
                .kafKaConfig(KafkaConfigEnum.TEST)
                .messageTopic(KafkaConstants.LOCATIONS_TOPIC)
                .messageGroup(KafkaConstants.TEST_CONSUMER_GROUP)
                .flinkCheckpointConfig(FlinkUtils.FlinkCheckpointConfig.builder().build())
                .build());
        StreamExecutionEnvironment env = flinkKafkaInitInfo.getEnv();
        DataStream<String> messageStream = flinkKafkaInitInfo.getMessageStream();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.26.55.116:9092");
        //增加配置属性操作如下：
        properties.setProperty("transaction.timeout.ms", String.valueOf(1000*60*5));

        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer(
                "testTopic",                  // target topic
                new KafkaSerializationSchemaWrapper<String>("testTopic", new FlinkKafkaPartitioner<String>() {
                    @Override
                    public int partition(String record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
                        return 0;
                    }
                }, true, new SimpleStringSchema()),    // serialization schema
                properties,                  // producer config
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

//        messageStream.print();
        messageStream.addSink(myProducer);
        env.execute("kafka-flink-Xsink");

    }

}
