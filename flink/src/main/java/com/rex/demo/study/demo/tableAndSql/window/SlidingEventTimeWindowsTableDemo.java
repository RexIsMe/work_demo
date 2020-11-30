package com.rex.demo.study.demo.tableAndSql.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * TODO Table API 实现滑动窗口(窗口大小10s,滑动步长2s)
 *
 * @author liuzebiao
 * @Date 2020-2-26 15:37
 */
public class SlidingEventTimeWindowsTableDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        DataStreamSource<String> streamSource = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Row> rowDataStream = streamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String line) throws Exception {
                String[] fields = line.split(",");
                Long time = Long.parseLong(fields[0]);
                String uid = fields[1];
                String pid = fields[2];
                Double money = Double.parseDouble(fields[3]);
                return Row.of(time, uid, pid, money);
            }
        }).returns(Types.ROW(Types.LONG, Types.STRING, Types.STRING, Types.DOUBLE));

        SingleOutputStreamOperator<Row> waterMarksRow = rowDataStream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Row>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(Row row) {
                return (long) row.getField(0);
            }
        });

        tableEnv.registerDataStream("t_orders", waterMarksRow, "atime,uid,pid,money,rowtime.rowtime");


        /******Flink Table API 实现窗口 start ******/
        Table table = tableEnv.scan("t_orders")
                .window(Slide.over("10.seconds").every("2.seconds").on("rowtime").as("win"))
                .groupBy("uid,win")
                .select("uid,win.start,win.end,win.rowtime,money.sum as total");
        /******Flink Table API 实现窗口 end ******/


        /******Flink SQL 实现窗口 start ******/
//        String sql = "select uid,sum(money),hop_end(rowtime,interval '2' SECOND,interval '10' second) as widEnd" +
//                " from t_orders group by hop(rowtime,interval '2' second,interval '10' second),uid";
//
//        Table table = tableEnv.sqlQuery(sql);
        /******Flink SQL 实现窗口 end ******/


        DataStream<Row> dataStream = tableEnv.toAppendStream(table, Row.class);

        dataStream.print();

        env.execute("TumblingEventTimeWindowsTable");
    }
}

