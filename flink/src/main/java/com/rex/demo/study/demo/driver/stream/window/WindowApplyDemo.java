package com.rex.demo.study.demo.driver.stream.window;

import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * TODO 使用 Window 窗口的 apply 方法，实现一个 sum()求和方法
 *
 * @author liuzebiao
 * @Date 2020-2-21 14:03
 */
public class WindowApplyDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> intStream = streamSource.map(Integer::parseInt);

        AllWindowedStream<Integer, TimeWindow> windowedStream = intStream.windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)));

        /**1.通过 apply() 方法，实现一个sum() 求和的功能**/
        SingleOutputStreamOperator<Integer> applyStream = windowedStream.apply(new AllWindowFunction<Integer, Integer, TimeWindow>() {
            @Override
            public void apply(TimeWindow timeWindow, Iterable<Integer> values, Collector<Integer> out) throws Exception {
                int sum = 0;
                for (Integer t : values) {
                    sum += t;
                }
                out.collect(sum);
            }
        });

        /**2.使用.sum() 方法**/
//        SingleOutputStreamOperator<Integer> applyStream = windowedStream.sum(0);

        applyStream.print();

        env.execute("WindowApplyDemo");
    }
}

