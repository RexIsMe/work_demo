package com.rex.demo.study.demo.enums;

/**
 * kafka配置枚举类
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */
public enum KafkaConfigEnum {

    TEST("172.26.55.116:9092"),
    PROD("172.26.55.116:9092");

    private String bootstrapServers;

    KafkaConfigEnum(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }
}
