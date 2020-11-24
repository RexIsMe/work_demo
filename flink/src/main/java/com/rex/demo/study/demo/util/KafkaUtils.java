package com.rex.demo.study.demo.util;

import com.rex.demo.study.demo.constants.KafkaConstants;
import com.rex.demo.study.demo.enums.KafkaConfigEnum;

import java.util.Properties;

/**
 * kafka 工具类
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */
public class KafkaUtils {
    private KafkaUtils() {
    }

    public static Properties getKafkaConsumerProperties(KafkaConfigEnum kafkaConfigEnum, String consumerGroup){
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", kafkaConfigEnum.getBootstrapServers());
        properties.setProperty("group.id", consumerGroup);
        properties.setProperty("key.deserializer", KafkaConstants.KEY_DESERIALIZER);
        properties.setProperty("value.deserializer", KafkaConstants.VALUE_DESERIALIZER);
//        properties.setProperty("auto.offset.reset", "latest");
        return properties;
    }

}
