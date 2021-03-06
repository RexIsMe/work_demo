package com.rex.demo.study.demo.util;

import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.net.URL;
import java.util.Properties;

/**
 * 一些公用方法
 *
 * @Author li zhiqang
 * @create 2020/11/25
 */
public class CommonUtils {

    /**
     * 此部分读取Socket数据，只是用来人为出现异常，触发重启策略。验证重启后是否会再次去读之前已读过的数据(Exactly-Once)
     */
    public static void diyThrowException(StreamExecutionEnvironment env){
        DataStreamSource<String> socketTextStream = env.socketTextStream("172.26.55.109", 8888);

        SingleOutputStreamOperator<String> streamOperator1 = socketTextStream.map(new MapFunction<String, String>() {
            @Override
            public String map(String word) throws Exception {
                if ("error".equals(word)) {
                    throw new RuntimeException("Throw Exception");
                }
                return word;
            }
        });
    }


    public static Properties getKafkaProperties(){
        //Kafka props
        Properties properties = new Properties();
        //指定Kafka的Broker地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.16.0.211:9092");
        //指定组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "testGroup");
        //如果没有记录偏移量，第一次从最开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //Kafka的消费者，不自动提交偏移量
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        return properties;
    }

    public static StreamExecutionEnvironment getEnv(){
        /**1.创建流运行环境**/
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //设置全局并行度，默认为CPU核数
        env.setParallelism(1);
        /**请注意此处：**/
        //1.只有开启了CheckPointing,才会有重启策略
        env.enableCheckpointing(20000);
        //2.默认的重启策略是：固定延迟无限重启
        //此处设置重启策略为：出现异常总共重启3次，隔5秒一次；重启过3次则任务停止
        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));
        //系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //设置Checkpoint模式（与Kafka整合，一定要设置Checkpoint模式为Exactly_Once）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        return env;
    }

    /**
     * 获取项目resources的路径
     * @param fileName
     * @return
     */
    public static String getResourcePath(String fileName){
        return PropertiesConfiguration.PropertiesReader.class.getClassLoader().getResource(fileName).getPath();
    }

    public static void main(String[] args){
        System.out.println(System.currentTimeMillis());
    }

}
