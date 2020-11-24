package com.rex.demo.study.test;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;

/**
 * @Author li zhiqang
 * @create 2020/11/24
 */
public class Common {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> text = env.readTextFile("hdfs://hadoop-master:9000/test/data/json.txt");
        text.print();
//        env.execute();
    }

}
