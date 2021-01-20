/*
package com.rex.demo.study.demo.tableAndSql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;


*/
/**
 * TODO WordCount Table API Demo
 *
 * @author liuzebiao
 * @Date 2020-2-26 10:37
 *//*

public class BatchWordCountTableAPIDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建一个实时的 Table 执行上下文环境
        BatchTableEnvironment batchEnv = BatchTableEnvironment.create(env);

        DataSource<String> lines = env.readTextFile("C:\\Users\\a\\Desktop\\wordcount.txt");

        FlatMapOperator<String, Tuple2<String, Long>> wordDataSet = lines.flatMap((String line, Collector<Tuple2<String, Long>> out) ->
                Arrays.stream(line.split(",")).forEach(str -> out.collect(Tuple2.of(str, 1l)))
        ).returns(Types.TUPLE(Types.STRING, Types.LONG));


        //将 wordDataSet 流注册成表(表名为：word_count)
        Table table = batchEnv.fromDataSet(wordDataSet,"word,num");

        Table resultTable = table
                .groupBy("word")//分组
                .select("word, num.sum as counts")//sum求和
                .filter("counts >= 2")//过滤
                .orderBy("counts.desc");//排序

        //如下三种均可(WordCount为实体类，ROW为Flink提供的一个拥有多个属性的类)
        DataSet<Row> dataSet = batchEnv.toDataSet(resultTable, Types.ROW(Types.STRING, Types.LONG));

        dataSet.print();
    }
}

*/
