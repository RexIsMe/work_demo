package com.rex.demo.study.demo.util;

import com.rex.demo.study.demo.entity.FlinkInitInfo;
import com.rex.demo.study.demo.enums.DateEnum;
import com.rex.demo.study.demo.enums.KafkaConfigEnum;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Flink 工具类
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */
public class FlinkUtils {
    private FlinkUtils() {
    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FlinkCheckpointConfig implements Serializable {

        private static final long serialVersionUID = 2828849328747076877L;

        /**
         * Checkpoint存储定义
         */
        private StateBackend stateBackend;

        /**
         * 开启Checkpoint，每enableCheckpointing毫秒进行一次Checkpoint
         */
        private Long enableCheckpointing;

        /**
         * Checkpoint模式，例如 EXACTLY_ONCE
         */
        private CheckpointingMode checkpointingMode;

        /**
         * Checkpoint的超时时间
         */
        private Long checkpointTimeout;

        /**
         * 同一时间，只允许有maxConcurrentCheckpoints个Checkpoint在发生
         */
        private Integer maxConcurrentCheckpoints;

        /**
         * 两次Checkpoint之间的最小时间间隔为minPauseBetweenCheckpoints毫秒
         */
        private Long minPauseBetweenCheckpoints;

        /**
         * 当Flink任务取消时，保留外部保存的CheckPoint信息
         */
        private CheckpointConfig.ExternalizedCheckpointCleanup enableExternalizedCheckpoints;

        /**
         * 当有较新的Savepoint时，作业也会从Checkpoint处恢复（flink 1.9 开始支持）
         */
        private Boolean preferCheckpointForRecovery;

        /**
         * 作业最多允许Checkpoint失败1次（flink 1.9 开始支持）
         */
        private Integer tolerableCheckpointFailureNumber;

        /**
         * Checkpoint失败后，整个Flink任务也会失败（flink 1.9 之前）
         */
        private Boolean failTasksOnCheckpointingErrors;

    }

    @Data
    @Builder
    @NoArgsConstructor
    @AllArgsConstructor
    public static class FlinkStartConfig implements Serializable {

        private KafkaConfigEnum kafKaConfig;

        private String messageTopic;

        private List<String> messageTopicList;

        private String messageGroup;

        private RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration;

        private FlinkCheckpointConfig flinkCheckpointConfig;

    }

    /**
     * 获取
     * @param flinkStartConfig
     * @return
     */
    public static FlinkInitInfo getFlinkKafkaInitInfo(FlinkStartConfig flinkStartConfig) {
        StreamExecutionEnvironment env = getEnv(flinkStartConfig);
        FlinkKafkaConsumer<String> kafkaSource = StringUtils.isNotBlank(flinkStartConfig.getMessageTopic())
                ? new FlinkKafkaConsumer(flinkStartConfig.getMessageTopic(), new SimpleStringSchema(),
                KafkaUtils.getKafkaConsumerProperties(flinkStartConfig.getKafKaConfig(),
                        flinkStartConfig.getMessageGroup()))
                : new FlinkKafkaConsumer(flinkStartConfig.getMessageTopicList(), new SimpleStringSchema(), KafkaUtils
                .getKafkaConsumerProperties(flinkStartConfig.getKafKaConfig(), flinkStartConfig.getMessageGroup()));
        DataStream<String> messageStream = env.addSource(kafkaSource);
        return FlinkInitInfo.builder().env(env).messageStream(messageStream).build();
    }

    public static StreamExecutionEnvironment getEnv(FlinkStartConfig flinkStartConfig) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        /* 检查点设置、重启设置 */
        RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration = flinkStartConfig.getRestartStrategyConfiguration();
        env.setRestartStrategy(Optional.ofNullable(restartStrategyConfiguration).orElse(RestartStrategies.fixedDelayRestart(30, Time.of(5L, TimeUnit.SECONDS))));
        addCheckpointConfig(env, flinkStartConfig.getFlinkCheckpointConfig());
        return env;
    }

    public static void addCheckpointConfig(StreamExecutionEnvironment env, FlinkCheckpointConfig flinkCheckpointConfig) {
        if (flinkCheckpointConfig == null) {
            return;
        }
//        StateBackend stateBackend = flinkCheckpointConfig.getStateBackend();
//        if (stateBackend != null) {
//            env.setStateBackend(stateBackend);
//        }
        env.setStateBackend(Optional.ofNullable(flinkCheckpointConfig.getStateBackend()).orElse(new FsStateBackend("hdfs://hadoop-master:9000/test/flink/checkpoint")));
        //默认每10分钟执行一次
        env.enableCheckpointing(Optional.ofNullable(flinkCheckpointConfig.getEnableCheckpointing())
                .orElse(1 * DateUtils.getMillis(DateEnum.MINUTE)));
        //默认语义EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(Optional.ofNullable(flinkCheckpointConfig.getCheckpointingMode())
                .orElse(CheckpointingMode.EXACTLY_ONCE));
        //默认CheckPoint保存10分钟超时
        env.getCheckpointConfig().setCheckpointTimeout(Optional.ofNullable(flinkCheckpointConfig.getCheckpointTimeout())
                .orElse(1 * DateUtils.getMillis(DateEnum.MINUTE)));
        //默认同一时间，只允许有1个Checkpoint在发生
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(
                Optional.ofNullable(flinkCheckpointConfig.getMaxConcurrentCheckpoints()).orElse(1));
        //默认两次Checkpoint之间的最小时间间隔为10分钟
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(
                Optional.ofNullable(flinkCheckpointConfig.getMinPauseBetweenCheckpoints())
                        .orElse(1 * DateUtils.getMillis(DateEnum.MINUTE)));
        //系统异常退出或人为 Cancel 掉，不删除checkpoint数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(Optional.ofNullable(flinkCheckpointConfig.getEnableExternalizedCheckpoints()).orElse(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION));
        //设置Checkpoint模式（与Kafka整合，一定要设置Checkpoint模式为Exactly_Once）
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    }
}
