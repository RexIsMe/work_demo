package com.rex.demo.study.demo.driver.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 不使用 Flink 提供的sum()方法，对单词进行分组求和计算。
 * 通过keyedState 实现sum()一样的功能
 *
 * @author liuzebiao
 * @Date 2020-2-17 11:42
 */
public class KeyedStateDemo {

    public static void main(String[] args) throws Exception {
        //1.创建一个 flink steam 程序的执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.使用StreamExecutionEnvironment创建DataStream
        DataStreamSource<String> lines = env.socketTextStream( "172.26.55.109", 8888);

        //Transformation（s） 对数据进行处理操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) {
                //将每个单词与 1 组合，形成一个元组
                return Tuple2.of(word, 1);
            }
        });

        //进行分组聚合(keyBy：将key相同的分到一个组中)
        KeyedStream<Tuple2<String, Integer>, Tuple> keyedStream = wordAndOne.keyBy(0);

        /** 使用 KeyedState 通过中间状态求和  ----- start ----**/
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyedStream.map(new RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            //状态数据不参与序列化，添加 transient 修饰
            private transient ValueState<Integer> valueState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //获取 KeyedState 数据
                ValueStateDescriptor stateDescriptor = new ValueStateDescriptor("sum-key-state", Types.TUPLE(Types.STRING, Types.INT));

                valueState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> tuple2) throws Exception {
                //输入的单词
                String word = tuple2.f0;
                //输入的次数
                Integer count = tuple2.f1;
                //根据State获取中间数据
                Integer historyVal = valueState.value();

                //根据State中间数据，进行累加
                if (historyVal != null) {
                    historyVal += count;
                    //累加后,更新State数据
                    valueState.update(historyVal);
                } else {
                    valueState.update(count);
                }
                return Tuple2.of(word,valueState.value());
            }
        });

        /** 使用 KeyedState 通过中间状态求和  ----- end ----**/

        //Transformation 结束

        //3.调用Sink （Sink必须调用）
        summed.print();

        //启动(这个异常不建议try...catch... 捕获,因为它会抛给上层flink,flink根据异常来做相应的重启策略等处理)
        env.execute("KeyedStateDemo");
    }
}

