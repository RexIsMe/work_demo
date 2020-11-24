package com.rex.demo.study.demo.constants;

/**
 * @Author li zhiqang
 * @create 2020/11/24
 */
public class KafkaConstants {
    private KafkaConstants() {
    }

    /* 反序列化器 */
    public static final String KEY_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";
    public static final String VALUE_DESERIALIZER = "org.apache.kafka.common.serialization.StringDeserializer";

    /* 测试环境kafka topic name */
    public static final String TEST_TOPIC = "test";
    /* 测试环境kafka consumer group name */
    public static final String TEST_CONSUMER_GROUP = "test-consumer-group";

}
