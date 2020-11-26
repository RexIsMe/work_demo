package com.rex.demo.study.demo.driver;


import com.rex.demo.study.demo.constants.KafkaConstants;
import com.rex.demo.study.demo.entity.FlinkInitInfo;
import com.rex.demo.study.demo.enums.KafkaConfigEnum;
import com.rex.demo.study.demo.sink.MySqlTwoPhaseCommitSink;
import com.rex.demo.study.demo.util.CommonUtils;
import com.rex.demo.study.demo.util.FlinkUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;

/**
 * Flink实现Kafka到Mysql的Exactly-Once
 *
 * 二、实现思想
 * 这里简单说下这个类的作用就是实现这个类的方法：beginTransaction、preCommit、commit、abort，达到事件（preCommit）预提交的逻辑（当事件进行自己的逻辑处理后进行预提交，如果预提交成功之后才进行真正的（commit）提交，如果预提交失败则调用abort方法进行事件的回滚操作），结合flink的checkpoint机制，来保存topic中partition的offset。
 * 达到的效果我举个例子来说明下：比如checkpoint每10s进行一次，此时用FlinkKafkaConsumer011实时消费kafka中的消息，消费并处理完消息后，进行一次预提交数据库的操作，如果预提交没有问题，10s后进行真正的插入数据库操作，如果插入成功，进行一次checkpoint，flink会自动记录消费的offset，可以将checkpoint保存的数据放到hdfs中，如果预提交出错，比如在5s的时候出错了，此时Flink程序就会进入不断的重启中，重启的策略可以在配置中设置，当然下一次的checkpoint也不会做了，checkpoint记录的还是上一次成功消费的offset，本次消费的数据因为在checkpoint期间，消费成功，但是预提交过程中失败了，注意此时数据并没有真正的执行插入操作，因为预提交（preCommit）失败，提交（commit）过程也不会发生了。等你将异常数据处理完成之后，再重新启动这个Flink程序，它会自动从上一次成功的checkpoint中继续消费数据，以此来达到Kafka到Mysql的Exactly-Once。
 *
 * @Author li zhiqang
 * @create 2020/11/26
 */
@Slf4j
public class StreamDemoKafka2Mysql {

    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//
//        //设置并行度，为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
//        env.setParallelism(1);
//        //checkpoint设置
//        //每隔10s进行启动一个检查点【设置checkpoint的周期】
//        env.enableCheckpointing(10000);
//        //设置模式为：exactly_one，仅一次语义
//        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
//        //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000);
//        //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
//        env.getCheckpointConfig().setCheckpointTimeout(10000);
//        //同一时间只允许进行一次检查点
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
//        //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        // 设置将来访问 hdfs 的使用的用户名, 否则会出现权限不够
//        System.setProperty("HADOOP_USER_NAME", "hadoop");
//        // 设置文件服务状态后端，并制定checkpoint目录
//        env.setStateBackend(new FsStateBackend("hdfs://hadoop-master:9000/test/flink/checkpoint/"));

        // 设置将来访问 hdfs 的使用的用户名, 否则会出现权限不够
        System.setProperty("HADOOP_USER_NAME", "hadoop");
        log.info("PostViewFlink task start");
        StreamExecutionEnvironment env = FlinkUtils.getEnv(FlinkUtils.FlinkStartConfig.builder()
                .flinkCheckpointConfig(FlinkUtils.FlinkCheckpointConfig.builder().build())
                .build());

        //设置kafka消费参数
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.26.55.116:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "flink-consumer-group1");
        //kafka分区自动发现周期
//        props.put(FlinkKafkaConsumerBase.KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS, "3000");

        /*SimpleStringSchema可以获取到kafka消息，JSONKeyValueDeserializationSchema可以获取都消息的key,value，metadata:topic,partition，offset等信息*/
//         FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>("student", new SimpleStringSchema(), props);
        FlinkKafkaConsumer011<ObjectNode> kafkaConsumer011 = new FlinkKafkaConsumer011<>("student", new JSONKeyValueDeserializationSchema(true), props);

        //加入kafka数据源
        DataStreamSource<ObjectNode> streamSource = env.addSource(kafkaConsumer011);
//        streamSource.print();
        //数据传输到下游
        streamSource.addSink(new MySqlTwoPhaseCommitSink()).name("MySqlTwoPhaseCommitSinkDemo");


        CommonUtils.diyThrowException(env);



        //触发执行
        env.execute(StreamDemoKafka2Mysql.class.getName());

    }


}
