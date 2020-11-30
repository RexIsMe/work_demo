package com.rex.demo.study.demo.tableAndSql;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * TODO WordCount 使用 SQL Demo
 * flink version 1.9.1
 *
 * @author liuzebiao
 * @Date 2020-2-26 10:37
 */
public class StreamWordCountSQLAPIDemo{

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建一个实时的 Table 执行上下文环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> lines = env.socketTextStream("172.26.55.109", 8888);

        SingleOutputStreamOperator<String> wordStream = lines.flatMap((String line, Collector<String> out)->
                Arrays.stream(line.split(",")).forEach(out::collect)
        ).returns(Types.STRING);

        //将 wordStream 流注册成表(表名为：word_count)
        //多字段使用逗号分隔。如："word,count"
        tableEnv.registerDataStream("word_count",wordStream,"word");
        //SQL
        String sql = "select word t0,count(1) as t1 from word_count group by word";
        //执行SQL
        Table table = tableEnv.sqlQuery(sql);

        //如下三种均可(WordCount为实体类，ROW为Flink提供的一个拥有多个属性的类)
//        DataStream<Tuple2<Boolean, Tuple2<String,Long>>> dataStream = tableEnv.toRetractStream(table, Types.TUPLE(Types.STRING, Types.LONG));
//        DataStream<Tuple2<Boolean, Tuple2<String,Long>>> dataStream = tableEnv.toRetractStream(table, WordCount.class);
        DataStream<Tuple2<Boolean, Row>> dataStream = tableEnv.toRetractStream(table, Types.ROW(Types.STRING, Types.LONG));

        dataStream.print();

        env.execute("StreamWordCountSQLAPIDemo");

    }
}

