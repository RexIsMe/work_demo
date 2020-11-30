package com.rex.demo.study.demo.util;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * TODO FlinkUtils工具类(持续更新编写)
 *
 * @author liuzebiao
 * @Date 2020-2-18 9:11
 */
public class FlinkUtils2 {

    private static StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

    /**
     * 返回 Flink流式环境
     * @return
     */
    public static StreamExecutionEnvironment getEnv(){
        return env;
    }

    /**
     * Flink 从 Kafka 中读取数据(满足Exactly-Once)
     * @param parameters
     * @param clazz
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> DataStream<T> createKafkaStream(ParameterTool parameters, Class<? extends DeserializationSchema> clazz) throws IllegalAccessException, InstantiationException {

        //设置全局参数
        env.getConfig().setGlobalJobParameters(parameters);
        //1.只有开启了CheckPointing,才会有重启策略
        //设置Checkpoint模式（与Kafka整合，一定要设置Checkpoint模式为Exactly_Once）
        env.enableCheckpointing(parameters.getLong("checkpoint.interval",5000L), CheckpointingMode.EXACTLY_ONCE);
        //2.默认的重启策略是：固定延迟无限重启
        //此处设置重启策略为：出现异常重启3次，隔5秒一次(你也可以在flink-conf.yaml配置文件中写死。此处配置会覆盖配置文件中的)
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(20)));
        //系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**2.Source:读取 Kafka 中的消息**/
        //Kafka props
        Properties properties = new Properties();
        //指定Kafka的Broker地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameters.getRequired("bootstrap.server"));
        //指定组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, parameters.getRequired("group.id"));
        //如果没有记录偏移量，第一次从最开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, parameters.get("auto.offset.reset","earliest"));
        //Kafka的消费者，不自动提交偏移量
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, parameters.get("enable.auto.commit","false"));

        String topics = parameters.getRequired("topics");

        List<String> topicList = Arrays.asList(topics.split(","));

        FlinkKafkaConsumer011<T> kafkaConsumer = new FlinkKafkaConsumer011(topicList, clazz.newInstance(), properties);

        return env.addSource(kafkaConsumer);
    }

    /**
     * Flink 从 Kafka 中读取数据(满足Exactly-Once)
     * @param parameters
     * @param clazz
     * @param <T>
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static <T> DataStream<T> createKafkaStream(ParameterTool parameters,String topics,String groupId, Class<? extends DeserializationSchema> clazz) throws IllegalAccessException, InstantiationException {

        //设置全局参数
        env.getConfig().setGlobalJobParameters(parameters);
        //1.只有开启了CheckPointing,才会有重启策略
        //设置Checkpoint模式（与Kafka整合，一定要设置Checkpoint模式为Exactly_Once）
        env.enableCheckpointing(parameters.getLong("checkpoint.interval",5000L),CheckpointingMode.EXACTLY_ONCE);
        //2.默认的重启策略是：固定延迟无限重启
        //此处设置重启策略为：出现异常重启3次，隔5秒一次(你也可以在flink-conf.yaml配置文件中写死。此处配置会覆盖配置文件中的)
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(10, Time.seconds(20)));
        //系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        /**2.Source:读取 Kafka 中的消息**/
        //Kafka props
        Properties properties = new Properties();
        //指定Kafka的Broker地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, parameters.getRequired("bootstrap.server"));
        //指定组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        //如果没有记录偏移量，第一次从最开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, parameters.get("auto.offset.reset","earliest"));
        //Kafka的消费者，不自动提交偏移量
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, parameters.get("enable.auto.commit","false"));

        List<String> topicList = Arrays.asList(topics.split(","));
        FlinkKafkaConsumer011<T> kafkaConsumer = new FlinkKafkaConsumer011(topicList, clazz.newInstance(), properties);

        return env.addSource(kafkaConsumer);
    }
}

