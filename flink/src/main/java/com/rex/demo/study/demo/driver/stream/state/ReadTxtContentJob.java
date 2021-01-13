package com.rex.demo.study.demo.driver.stream.state;

import com.rex.demo.study.demo.source.MyExactlyOnceParFileSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * TODO 从自定义Source中读取文件中的内容，即实现 tail -f 的功能。
 *
 * @author liuzebiao
 * @Date 2020-2-17 13:26
 */
public class ReadTxtContentJob {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //1.只有开启了CheckPointing,才会有重启策略
        env.enableCheckpointing(5000);
        //2.设置并行度为2,因为读取的文件夹中就存放了2个文件(0.txt和1.txt)
        env.setParallelism(2);

        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        /**此部分读取Socket数据，只是用来人为出现异常，触发重启策略。验证重启后是否会再次去读之前已读过的数据(Exactly-Once)*/
        /*************** start **************/
        DataStreamSource<String> socketTextStream = env.socketTextStream("172.26.55.109", 8888);
        SingleOutputStreamOperator<String> streamOperator = socketTextStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String word) throws Exception {
                if ("exception".equals(word)) {
                    throw new RuntimeException("Throw Exception");
                }
                return word;
            }
        });
        /************* end **************/
        //3.读取自定义 Source 中的数据
        DataStreamSource<Tuple2<String, String>> streamSource = env.addSource(new MyExactlyOnceParFileSource());
        //4.Sink部分，直接输出至控制台
        streamSource.print();

        env.execute("ReadTxtContentJob");
    }
}


