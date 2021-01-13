package com.rex.demo.study.demo.driver.stream;

import com.alibaba.fastjson.JSONObject;
import com.rex.demo.study.demo.constants.KafkaConstants;
import com.rex.demo.study.demo.entity.DCRecordEntity;
import com.rex.demo.study.demo.entity.FlinkInitInfo;
import com.rex.demo.study.demo.enums.KafkaConfigEnum;
import com.rex.demo.study.demo.util.FlinkUtils;
import com.rex.demo.study.demo.util.Md5Utils;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * 消费kafka数据并输出到常见sink
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */
@Slf4j
public class NormalSink {

    @SneakyThrows
    public static void main(String[] args){
        // 设置将来访问 hdfs 的使用的用户名, 否则会出现权限不够
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        log.info("PostViewFlink task start");
        FlinkInitInfo flinkKafkaInitInfo = FlinkUtils.getFlinkKafkaInitInfo(FlinkUtils.FlinkStartConfig.builder()
                .kafKaConfig(KafkaConfigEnum.TEST)
                .messageTopic(KafkaConstants.TEST_TOPIC)
                .messageGroup(KafkaConstants.TEST_CONSUMER_GROUP)
                .flinkCheckpointConfig(FlinkUtils.FlinkCheckpointConfig.builder().build())
                .build());
        StreamExecutionEnvironment env = flinkKafkaInitInfo.getEnv();
        DataStream<String> messageStream = flinkKafkaInitInfo.getMessageStream();
        messageStream
                .rebalance()
                .map(new DCRecordEntityMapFunction())
//                .addSink(new DCRecordEntitySink());
                .writeUsingOutputFormat(new MysqlSink1());
//                .print();
        env.execute("kafka-flink-Xsink");
    }


    /**
     * 自定义map算子逻辑
     * 将从kafka中消费到的String message转换成DCRecordEntity对象
     */
    static class DCRecordEntityMapFunction implements MapFunction<String, DCRecordEntity> {

        @Override
        public DCRecordEntity map(String value) {
            if (StringUtils.isBlank(value)) {
                return null;
            }
            try {
                return JSONObject.parseObject(value, DCRecordEntity.class);
            } catch (Exception e) {
                log.error("DCRecordEntity Function map error value={}", value, e);
            }
            return null;
        }
    }

    /**
     * 自定义DCRecordEntity的sink
     * 将处理结果保存到mysql
     */
    static class DCRecordEntitySink extends RichSinkFunction<DCRecordEntity> {
        Connection conn;
        PreparedStatement ps;
        String jdbcUrl = "jdbc:mysql://172.26.55.109:3306/dc?autoReconnect=true&autoReconnectForPools=true&useUnicode=true&characterEncoding=utf8&useSSL=false&useAffectedRows=true&allowMultiQueries=true";
        String username = "root";
        String password = "t4*9&/y?c,h.e17!";
        String driverName = "com.mysql.jdbc.Driver";

        @Override
        public void open(Configuration parameters) {
            try{
//                Class.forName(driverName);
                conn = DriverManager.getConnection(jdbcUrl, username, password);
                // close auto commit
                conn.setAutoCommit(false);
            } catch (Exception e) {
                log.error("连接 mysql 失败", e);
                System.exit(-1);
            }

        }

        @Override
        public void close() {
            if (conn != null){
                try {
                    conn.commit();
                    conn.close();
                } catch (SQLException e) {
                    log.error("操作 mysql 失败", e);
                }
            }
        }

        @Override
        public void invoke(DCRecordEntity value, Context context) throws Exception {
            System.out.println(("get DCRecordEntity : " + value.toString()));
            ps = conn.prepareStatement("insert into dc.part_stock_realtime(md5_key, organization_id, sum_inventory_number) values(?,?,?)");
            ps.setString(1, Md5Utils.stringToMD5(value.getTimestamp().toString()));
            ps.setLong(2, value.getDataList().get(0).getLong("organizationId"));
            ps.setInt(3, value.getDataList().get(0).getInteger("inventoryNumber"));

            ps.execute();
            conn.commit();
        }
    }

    /**
     * 通过实现OutputFormat接口方式得到sink
     */
    static class MysqlSink1 implements OutputFormat<DCRecordEntity>{
        Connection conn;
        PreparedStatement ps;
        String jdbcUrl = "jdbc:mysql://172.26.55.109:3306/dc?autoReconnect=true&autoReconnectForPools=true&useUnicode=true&characterEncoding=utf8&useSSL=false&useAffectedRows=true&allowMultiQueries=true";
        String username = "root";
        String password = "t4*9&/y?c,h.e17!";
        String driverName = "com.mysql.jdbc.Driver";

        @Override
        public void configure(Configuration parameters) {
            // not need
        }

        @Override
        public void open(int taskNumber, int numTasks) throws IOException {
            /* 同DCRecordEntitySink */
        }

        @Override
        public void writeRecord(DCRecordEntity record) throws IOException {
            /* 同DCRecordEntitySink */
        }

        @Override
        public void close() throws IOException {
            /* 同DCRecordEntitySink */
        }
    }


}
