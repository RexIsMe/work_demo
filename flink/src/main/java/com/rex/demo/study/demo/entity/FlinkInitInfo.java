package com.rex.demo.study.demo.entity;

import lombok.Builder;
import lombok.Data;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * flink 初始化对象
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */
@Data
@Builder
public class FlinkInitInfo {

    private StreamExecutionEnvironment env;
    private DataStream<String> messageStream;
    private List<DataStream<String>> messageStreamList;

}
