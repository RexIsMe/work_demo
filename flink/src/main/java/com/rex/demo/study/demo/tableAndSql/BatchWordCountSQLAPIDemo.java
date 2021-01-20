/*
package com.rex.demo.study.demo.tableAndSql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;

*/
/**
 * TODO 离线 WordCount SQL Demo
 *
 * @author liuzebiao
 * @Date 2020-2-26 10:37
 *//*

public class BatchWordCountSQLAPIDemo {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //创建一个实时的 Table 执行上下文环境
        BatchTableEnvironment batchEnv = BatchTableEnvironment.create(env);

        DataSource<String> lines = env.readTextFile("C:\\Users\\Administrator\\Desktop\\bigData\\flink\\test\\0.txt");

        FlatMapOperator<String, String> wordDataSet = lines.flatMap((String line, Collector<String> out) ->
                Arrays.stream(line.split(",")).forEach(out::collect)
        ).returns(Types.STRING);

        //将 wordDataSet 流注册成表(表名为：word_count)
        batchEnv.registerDataSet("word_count",wordDataSet,"word");
        //SQL
        String sql = "select word,count(1) from word_count group by word having count(1) >= 2 order by count(1) desc";
        //执行SQL
        Table table = batchEnv.sqlQuery(sql);

        //如下三种均可(WordCount为实体类，ROW为Flink提供的一个拥有多个属性的类)
        DataSet<Row> dataSet = batchEnv.toDataSet(table, Types.ROW(Types.STRING, Types.LONG));

        dataSet.printToErr();

    }
}

*/
