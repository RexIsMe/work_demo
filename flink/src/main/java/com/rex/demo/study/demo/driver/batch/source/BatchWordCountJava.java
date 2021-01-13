package com.rex.demo.study.demo.driver.batch.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * 将集合中的元素分组、计数后写出到文件
 *
 * @Author li zhiqang
 * @create 2021/1/11
 */
public class BatchWordCountJava {
    //获取运行环境
    private static ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
    private static String outPath = "C:\\Users\\Administrator\\Desktop\\bigData\\flink\\test\\1.txt";

    public static void main(String[] args) throws Exception{
//        DataSource dataSource = collectionSource();
        DataSource dataSource = flieSource();
        wordCount(dataSource, outPath);
    }


    /**
     * 文件数据源
     * @return
     */
    public static DataSource flieSource(){
        String inputPath = "C:\\Users\\Administrator\\Desktop\\bigData\\flink\\test\\0.txt";
        //获取文件中的内容
        return env.readTextFile(inputPath);
    }

    /**
     * 从集合中获取数据源
     * @return
     */
    public static DataSource collectionSource(){
        String[] arrData = new String[]{"10","15","20"};
        List data = Arrays.asList(arrData);
        return env.fromCollection(data);
    }

    /**
     * 对数据源做wordCount
     * @param ds
     * @param outPath
     * @throws Exception
     */
    public static void wordCount(DataSource ds, String outPath) throws Exception {
        DataSet<Tuple2<String, Integer>> counts = ds.flatMap(new Tokenizer()).groupBy(0).sum(1);
        counts.writeAsCsv(outPath,"\n"," ", FileSystem.WriteMode.OVERWRITE).setParallelism(1);
        env.execute("batch word count");
    }


    public static class Tokenizer implements FlatMapFunction<String,Tuple2<String,Integer>>{
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
            String[] tokens = value.toLowerCase().split("\\W+");
            for (String token: tokens) {
                if(token.length()>0){
                    out.collect(new Tuple2<String, Integer>(token,1));
                }
            }
        }
    }

}
