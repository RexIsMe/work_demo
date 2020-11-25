package com.rex.demo.study.demo.test;

/**
 * Flink 实时读取 Kafka 中的数据，并保证 Exaclty-Once。即：当任务出现异常，触发重启策略任务被重启后，对已经消费过的消息不进行重复消费。
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */

import com.rex.demo.study.demo.util.CommonUtils;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * TODO Flink整合 Kafka，实现 Exactly-Once
 *
 * @author liuzebiao
 * @Date 2020-2-15 9:53
 */
public class RestartStrategyDemo2 {

    /**
     * 显式设置状态后端，将checkpoint保存到外部存储组件-hdfs
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        // 设置将来访问 hdfs 的使用的用户名, 否则会出现权限不够
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        /**1.创建流运行环境**/
        StreamExecutionEnvironment env = CommonUtils.getEnv();
        env.setStateBackend(new FsStateBackend("hdfs://hadoop-master:9000/test/flink/checkpoint/"));

        /**2.Source:读取 Kafka 中的消息**/
        Properties properties = CommonUtils.getKafkaProperties();

        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer("testTopic", new SimpleStringSchema(), properties);
        /**
         * ①你如果禁用CheckPointing，则Flink Kafka Consumer依赖于内部使用的Kafka客户端的自动定期偏移量提交功能。该偏移量会被记录在 Kafka 中的 _consumer_offsets 这个特殊记录偏移量的 Topic 中。
         * ②你如果启用CheckPointing，偏移量则会被记录在 StateBackend 中。该方法kafkaSource.setCommitOffsetsOnCheckpoints(boolean);设置为 ture 时，偏移量会在 StateBackend 和 Kafka 中的 _consumer_offsets Topic 中都会记录一份；设置为 false 时，偏移量只会在 StateBackend 中的 存储一份。
         */
        kafkaSource.setCommitOffsetsOnCheckpoints(true);

        //Checkpoint成功后，还要向Kafka特殊的topic中写偏移量(此处不建议改为false )
        //设置为false后，则不会向特殊topic中写偏移量。
        //kafkaSource.setCommitOffsetsOnCheckpoints(false);
        //通过addSource()方式，创建 Kafka DataStream
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaSource);

        /**3.Transformation过程**/
        SingleOutputStreamOperator<Tuple2<String, Integer>> streamOperator = kafkaDataStream.map(str -> Tuple2.of(str, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
        //对元组 Tuple2 分组求和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = streamOperator.keyBy(0).sum(1);

        CommonUtils.diyThrowException(env);

        /**4.Sink过程**/
        sum.print();

        /**5.任务执行**/
        env.execute("RestartStrategyDemo");
    }
}


