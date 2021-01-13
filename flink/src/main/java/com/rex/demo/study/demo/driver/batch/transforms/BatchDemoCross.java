package com.rex.demo.study.demo.driver.batch.transforms;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.CrossOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.core.fs.FileSystem;

import java.util.ArrayList;

/**
 * 获取笛卡尔积
 *
 * Created by xuwei.tech on 2018/10/8.
 */
public class BatchDemoCross {

    private static String outPath = "C:\\Users\\Administrator\\Desktop\\bigData\\flink\\test\\cross.txt";

    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //tuple2<用户id，用户姓名>
        ArrayList<String> data1 = new ArrayList<>();
        data1.add("zs");
        data1.add("ww");

        //tuple2<用户id，用户所在城市>
        ArrayList<Integer> data2 = new ArrayList<>();
        data2.add(1);
        data2.add(2);

        DataSource<String> text1 = env.fromCollection(data1);
        DataSource<Integer> text2 = env.fromCollection(data2);

        CrossOperator.DefaultCross<String, Integer> cross = text1.cross(text2);

        cross.print();
        cross.writeAsCsv(outPath, "\n", "", FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        env.execute("getCross job");
    }



}
