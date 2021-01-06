package com.rex.demo.study.demo.driver;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * Flink 官方资料
 * 执行流程学习
 *
 * @Author li zhiqang
 * @create 2020/12/28
 */
public class WindowWordCount {

    public static void main(String[] args) throws Exception {

        if(args == null || args.length == 0){
            args = new String[2];
            args[0] = "--input";
            args[1] = "C:\\Users\\Administrator\\Desktop\\bigData\\data\\1.txt";
        }

        //命令行启动，参数处理
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(parameterTool);

        final int windowSize = parameterTool.getInt("window", 10);
        final int slideSize = parameterTool.getInt("slide", 10);

        DataStreamSource<String> input = env.readTextFile(parameterTool.get("input")).setParallelism(2);
        input.printToErr();

        SingleOutputStreamOperator<String> flatMap_sg = input.flatMap((FlatMapFunction<String, String>) (value, out) -> {
            String[] splits = value.split("\\s");
            for (String word : splits) {
                out.collect(word);
            }
        }).setParallelism(4)
                //在集群环境中才有用
//                .slotSharingGroup("flatMap_sg")
                .returns(Types.STRING);

        flatMap_sg.printToErr();

//        SingleOutputStreamOperator<String> counts = flatMap_sg
//                .keyBy(0)
//                .countWindow(windowSize, slideSize)
//                .sum(1).setParallelism(3).slotSharingGroup("sum_sg");
//
//        counts.print().setParallelism(3);

        env.execute("WindowWordCount");
    }

}
