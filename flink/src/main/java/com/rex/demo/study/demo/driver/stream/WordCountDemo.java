package com.rex.demo.study.demo.driver.stream;

import com.rex.demo.study.demo.util.CommonUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.configuration.description.ListElement.list;

/**
 * 最基础的wordCount示例
 *
 * @Author li zhiqang
 * @create 2020/12/28
 */
public class WordCountDemo {

    public static void main(String[] args) throws Exception {
        /**1.创建流运行环境**/
        StreamExecutionEnvironment env = CommonUtils.getEnv();

        List list = new ArrayList<String>();
        list.add("aa bb cc");
        list.add("aa");
        list.add("bb");
        list.add("vv");
        list.add("cc");
        list.add("dd");
        list.add("dd");
        DataStreamSource dataStreamSource = env.fromCollection(list);

        SingleOutputStreamOperator singleOutputStreamOperator = dataStreamSource.flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split("\\s");
                for (String word:splits) {
                    out.collect(word);
                }
            }
        });//打平操作，把每行的单词转为<word,count>类型的数据
        singleOutputStreamOperator.printToErr();
        SingleOutputStreamOperator returns = singleOutputStreamOperator
                .map(str -> Tuple2.of(str, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        SingleOutputStreamOperator sum = returns.keyBy(0).sum(1);
//
        sum.printToErr();

        env.execute();
    }

}
