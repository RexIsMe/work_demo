package com.rex.demo.study.demo.driver.batch.transforms;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

import java.util.ArrayList;


/**
 *
 * 按指定条件去重
 *
 */
public class BatchDemoDistinct {

    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        ArrayList<String> data = new ArrayList<>();
        data.add("hello you");
        data.add("hello me");

        DataSource<String> text = env.fromCollection(data);

//        FlatMapOperator<String, String> flatMapData = text.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String value, Collector<String> out) throws Exception {
//                String[] split = value.toLowerCase().split("\\W+");
//                for (String word : split) {
//                    System.out.println("单词："+word);
//                    out.collect(word);
//                }
//            }
//        });
//
//        flatMapData.printToErr();
//        flatMapData.distinct()// 对数据进行整体去重
//                .print();


        MapOperator<String, Tuple2<String, String>> map = text.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String value) throws Exception {
                String[] split = value.split("\\W+");
                return new Tuple2<>(split[0], split[1]);
            }
        });

        map.printToErr();
        //指定规则去重；根据Tuple2的第一个字段
        map.distinct(0).print();
    }



}
