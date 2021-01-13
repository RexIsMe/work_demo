package com.rex.demo.study.demo.driver.batch.transforms;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * Distributed Cache
 *
 * 文件缓存使用示例
 *
 */
public class BatchDemoDisCache {

    public static void main(String[] args) throws Exception{

        //获取运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //1：注册一个文件,可以使用hdfs或者s3上的文件
//        env.registerCachedFile("hdfs://hadoop-master:9000/test/data/json.txt","a.txt");
        env.registerCachedFile("C:\\Users\\Administrator\\Desktop\\bigData\\flink\\test\\cross.txt","a.txt");

        DataSource<String> data = env.fromElements("a", "b", "c", "d");

        DataSet<String> result = data.map(new RichMapFunction<String, String>() {
            private ArrayList<String> dataList = new ArrayList<String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                //2：使用文件
                File myFile = getRuntimeContext().getDistributedCache().getFile("a.txt");
                List<String> lines = FileUtils.readLines(myFile);
                for (String line : lines) {
                    this.dataList.add(line);
                    System.out.println("line:" + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                int indexVal = value.toCharArray()[0];
                int label = 'a';
                //在这里就可以使用dataList
                return value + ":" + dataList.get(indexVal - label);
            }
        });

        result.print();

    }

}
