//package com.rex.demo.study.kuaikan;
//
//import cn.hutool.core.date.DateUnit;
//import com.google.common.collect.Lists;
//import com.kuaikan.common.perfcounter.PerfCounter;
//import com.kuaikan.data.flink.common.bean.ComicBean;
//import com.kuaikan.data.flink.common.elasticsearch.ElasticsearchCommon.ComicInfoConfig;
//import com.kuaikan.data.flink.common.elasticsearch.ElasticsearchUtil;
//import com.kuaikan.data.flink.common.elasticsearch.ElasticsearchUtil.ElasticsearchConfigEnum;
//import com.kuaikan.data.flink.common.enums.TopicAndApiEnum;
//import com.kuaikan.data.flink.common.kafka.KafkaUtil;
//import com.kuaikan.data.flink.common.kafka.KafkaUtil.KafKaConfigEnum;
//import com.kuaikan.data.flink.common.perfcounter.BaseMonitorMessage;
//import com.kuaikan.data.flink.common.perfcounter.BaseMonitorMetric;
//import com.kuaikan.data.flink.common.perfcounter.BaseMonitorType;
//import com.kuaikan.data.flink.common.rocketmq.RocketMqUtil;
//import com.kuaikan.data.flink.common.rocketmq.RocketMqUtil.RocketMqConfigEnum;
//import lombok.AllArgsConstructor;
//import lombok.Builder;
//import lombok.Data;
//import lombok.NoArgsConstructor;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.state.MapStateDescriptor;
//import org.apache.flink.api.common.time.Time;
//import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
//import org.apache.flink.runtime.state.StateBackend;
//import org.apache.flink.streaming.api.CheckpointingMode;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
//import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.rocketmq.flink.RocketMQSource;
//import org.elasticsearch.action.search.ClearScrollRequest;
//import org.elasticsearch.action.search.ClearScrollResponse;
//import org.elasticsearch.action.search.SearchRequest;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.action.search.SearchScrollRequest;
//import org.elasticsearch.client.RequestOptions;
//import org.elasticsearch.client.RestHighLevelClient;
//import org.elasticsearch.common.unit.TimeValue;
//import org.elasticsearch.search.Scroll;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.builder.SearchSourceBuilder;
//
//import java.io.Serializable;
//import java.util.List;
//import java.util.Map;
//import java.util.Optional;
//import java.util.Properties;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.atomic.AtomicLong;
//import java.util.stream.Collectors;
//
///**
// * @author weizhiyu
// * @create 2019-07-16 18:50
// */
//@Slf4j
//public class FlinkUtil {
//
//    @Data
//    @Builder
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class FlinkCheckpointConfig implements Serializable {
//
//        private static final long serialVersionUID = 2828849328747076877L;
//
//        /**
//         * Checkpoint存储定义
//         */
//        private StateBackend stateBackend;
//
//        /**
//         * 开启Checkpoint，每enableCheckpointing毫秒进行一次Checkpoint
//         */
//        private Long enableCheckpointing;
//
//        /**
//         * Checkpoint模式，例如 EXACTLY_ONCE
//         */
//        private CheckpointingMode checkpointingMode;
//
//        /**
//         * Checkpoint的超时时间
//         */
//        private Long checkpointTimeout;
//
//        /**
//         * 同一时间，只允许有maxConcurrentCheckpoints个Checkpoint在发生
//         */
//        private Integer maxConcurrentCheckpoints;
//
//        /**
//         * 两次Checkpoint之间的最小时间间隔为minPauseBetweenCheckpoints毫秒
//         */
//        private Long minPauseBetweenCheckpoints;
//
//        /**
//         * 当Flink任务取消时，保留外部保存的CheckPoint信息
//         */
//        private ExternalizedCheckpointCleanup enableExternalizedCheckpoints;
//
//        /**
//         * 当有较新的Savepoint时，作业也会从Checkpoint处恢复（flink 1.9 开始支持）
//         */
//        private Boolean preferCheckpointForRecovery;
//
//        /**
//         * 作业最多允许Checkpoint失败1次（flink 1.9 开始支持）
//         */
//        private Integer tolerableCheckpointFailureNumber;
//
//        /**
//         * Checkpoint失败后，整个Flink任务也会失败（flink 1.9 之前）
//         */
//        private Boolean failTasksOnCheckpointingErrors;
//
//    }
//
//    @Data
//    @Builder
//    @NoArgsConstructor
//    @AllArgsConstructor
//    public static class FlinkStartConfig implements Serializable {
//
//        private KafKaConfigEnum kafKaConfig;
//
//        private String messageTopic;
//
//        private List<String> messageTopicList;
//
//        private String messageGroup;
//
//        private RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration;
//
//        private FlinkCheckpointConfig flinkCheckpointConfig;
//
//        private RocketMqConfigEnum rocketMqConfig;
//
//    }
//
//    @Deprecated
//    public static FlinkInitInfo getFlinkKafkaInitInfo(KafKaConfigEnum kafKaConfig, String kafkaTopic, String kafkaGroup,
//            int flinkRestartCount, long flinkRestartTime, TimeUnit flinkRestartTimeUnit) {
//        return getFlinkKafkaInitInfo(FlinkStartConfig.builder()
//                .kafKaConfig(kafKaConfig)
//                .messageTopic(kafkaTopic)
//                .messageGroup(kafkaGroup)
//                .build());
//    }
//
//    @Deprecated
//    public static FlinkInitInfo getFlinkKafkaInitInfoWithComicEsBroadcastStream(
//            KafkaUtil.KafKaConfigEnum kafKaConfigEnum, String kafkaTopic, String kafkaGroup, int flinkRestartCount,
//            int flinkRestartTime, TimeUnit flinkRestartTimeUnit,
//            BroadcastProcessFunction<String, List<ComicBean>, String> broadcastProcessFunction) {
//        FlinkInitInfo flinkKafkaInitInfo = getFlinkKafkaInitInfo(kafKaConfigEnum, kafkaTopic, kafkaGroup,
//                flinkRestartCount, flinkRestartTime, flinkRestartTimeUnit);
//        StreamExecutionEnvironment env = flinkKafkaInitInfo.getEnv();
//        DataStream<String> messageStream = flinkKafkaInitInfo.getMessageStream();
//        BroadcastStream<List<ComicBean>> comicBroadcastStream = getComicEsBroadcastStream(env);
//        messageStream.connect(comicBroadcastStream).process(broadcastProcessFunction).print();
//        return flinkKafkaInitInfo;
//    }
//
//    public static FlinkInitInfo getFlinkKafkaInitInfo(FlinkStartConfig flinkStartConfig) {
//        StreamExecutionEnvironment env = getEnv(flinkStartConfig);
//        FlinkKafkaConsumer<String> kafkaSource = StringUtils.isNotBlank(flinkStartConfig.getMessageTopic())
//                ? new FlinkKafkaConsumer(flinkStartConfig.getMessageTopic(), new SimpleStringSchema(),
//                        KafkaUtil.getConsumeProperties(flinkStartConfig.getKafKaConfig(),
//                                flinkStartConfig.getMessageGroup()))
//                : new FlinkKafkaConsumer(flinkStartConfig.getMessageTopicList(), new SimpleStringSchema(), KafkaUtil
//                        .getConsumeProperties(flinkStartConfig.getKafKaConfig(), flinkStartConfig.getMessageGroup()));
//        DataStream<String> messageStream = env.addSource(kafkaSource);
//        return FlinkInitInfo.builder().env(env).messageStream(messageStream).build();
//    }
//
//    public static FlinkInitInfo getFlinkKafkaInitInfoWithComicEsBroadcastStream(FlinkStartConfig flinkStartConfig,
//            BroadcastProcessFunction<String, List<ComicBean>, String> broadcastProcessFunction) {
//        FlinkInitInfo flinkKafkaInitInfo = getFlinkKafkaInitInfo(flinkStartConfig);
//        StreamExecutionEnvironment env = flinkKafkaInitInfo.getEnv();
//        DataStream<String> messageStream = flinkKafkaInitInfo.getMessageStream();
//        BroadcastStream<List<ComicBean>> comicBroadcastStream = getComicEsBroadcastStream(env);
//        messageStream.connect(comicBroadcastStream).process(broadcastProcessFunction).print();
//        return flinkKafkaInitInfo;
//    }
//
//    public static StreamExecutionEnvironment getEnv(FlinkStartConfig flinkStartConfig) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        RestartStrategies.RestartStrategyConfiguration restartStrategyConfiguration = flinkStartConfig
//                .getRestartStrategyConfiguration();
//        env.setRestartStrategy(Optional.ofNullable(restartStrategyConfiguration)
//                .orElse(RestartStrategies.fixedDelayRestart(30, Time.of(5L, TimeUnit.MINUTES))));
//        addCheckpointConfig(env, flinkStartConfig.getFlinkCheckpointConfig());
//        return env;
//    }
//
//    public static StreamExecutionEnvironment getEnv() {
//        return getEnv(FlinkStartConfig.builder().build());
//    }
//
//    public static void addCheckpointConfig(StreamExecutionEnvironment env,
//            FlinkCheckpointConfig flinkCheckpointConfig) {
//        if (flinkCheckpointConfig == null) {
//            return;
//        }
//        StateBackend stateBackend = flinkCheckpointConfig.getStateBackend();
//        if (stateBackend != null) {
//            env.setStateBackend(stateBackend);
//        }
//        //默认每10分钟执行一次
//        env.enableCheckpointing(Optional.ofNullable(flinkCheckpointConfig.getEnableCheckpointing())
//                .orElse(10 * DateUnit.MINUTE.getMillis()));
//        //默认语义EXACTLY_ONCE
//        env.getCheckpointConfig().setCheckpointingMode(Optional.ofNullable(flinkCheckpointConfig.getCheckpointingMode())
//                .orElse(CheckpointingMode.EXACTLY_ONCE));
//        //默认CheckPoint保存10分钟超时
//        env.getCheckpointConfig().setCheckpointTimeout(Optional.ofNullable(flinkCheckpointConfig.getCheckpointTimeout())
//                .orElse(10 * DateUnit.MINUTE.getMillis()));
//        //默认同一时间，只允许有1个Checkpoint在发生
//        env.getCheckpointConfig().setMaxConcurrentCheckpoints(
//                Optional.ofNullable(flinkCheckpointConfig.getMaxConcurrentCheckpoints()).orElse(1));
//        //默认两次Checkpoint之间的最小时间间隔为10分钟
//        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(
//                Optional.ofNullable(flinkCheckpointConfig.getMinPauseBetweenCheckpoints())
//                        .orElse(10 * DateUnit.MINUTE.getMillis()));
//    }
//
//    public static BroadcastStream<List<ComicBean>> getComicEsBroadcastStream(StreamExecutionEnvironment env) {
//        // 自定义广播流（单例）
//        final MapStateDescriptor<String, String> CONFIG_KEYWORDS = new MapStateDescriptor<>("config-keywords",
//                BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);
//        BroadcastStream<List<ComicBean>> comicBroadcastStream = env
//                .addSource(new RichSourceFunction<List<ComicBean>>() {
//
//                    private volatile boolean isRunning = true;
//
//                    private AtomicLong runCount = new AtomicLong(0L);
//
//                    @Override
//                    public void run(SourceContext<List<ComicBean>> ctx) throws Exception {
//                        RestHighLevelClient dataRecommendClient = ElasticsearchUtil
//                                .getClient(ElasticsearchConfigEnum.DATA_RECOMMEND);
//                        while (isRunning) {
//                            try {
//                                long totalCount = 0L;
//                                Scroll scroll = new Scroll(TimeValue.timeValueMinutes(10L));
//                                SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
//                                String[] columns = { ComicInfoConfig.TOPIC_ID, ComicInfoConfig.COMIC_ID };
//                                searchSourceBuilder.fetchSource(columns, null);
//                                searchSourceBuilder.size(10000);
//                                SearchRequest searchRequest = new SearchRequest();
//                                searchRequest.indices(ComicInfoConfig.INDEX_ALIAS_NAME);
//                                searchRequest.source(searchSourceBuilder);
//                                searchRequest.scroll(scroll);
//                                SearchResponse searchResponse = dataRecommendClient.search(searchRequest,
//                                        RequestOptions.DEFAULT);
//                                String scrollId = searchResponse.getScrollId();
//                                SearchHit[] searchHits = searchResponse.getHits().getHits();
//                                List<ComicBean> comicBeanList = Lists.newArrayList();
//                                while (searchHits != null && searchHits.length > 0) {
//                                    for (SearchHit searchHit : searchResponse.getHits()) {
//                                        Map<String, Object> map = searchHit.getSourceAsMap();
//                                        long topicId = Long.parseLong(map.get(ComicInfoConfig.TOPIC_ID).toString());
//                                        long comicId = Long.parseLong(map.get(ComicInfoConfig.COMIC_ID).toString());
//                                        comicBeanList
//                                                .add(ComicBean.builder().topicId(topicId).comicId(comicId).build());
//                                        totalCount += comicBeanList.size();
//                                    }
//                                    ctx.collect(comicBeanList);
//                                    SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
//                                    scrollRequest.scroll(scroll);
//                                    searchResponse = dataRecommendClient.scroll(scrollRequest, RequestOptions.DEFAULT);
//                                    searchHits = searchResponse.getHits().getHits();
//                                }
//                                ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
//                                clearScrollRequest.addScrollId(scrollId);
//                                ClearScrollResponse clearScrollResponse = dataRecommendClient
//                                        .clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
//                                boolean succeeded = clearScrollResponse.isSucceeded();
//                                log.info("FlinkUtil getComicEsBroadcastStream close scroll result={}", succeeded);
//                                log.info("FlinkUtil getComicEsBroadcastStream totalCount={}", totalCount);
//                            } catch (Exception e) {
//                                log.error("FlinkUtil getComicEsBroadcastStream error", e);
//                                PerfCounter.countGauge(BaseMonitorMetric.DATA_ONE_MONITOR,
//                                        BaseMonitorType.ERROR.getName(), "FlinkUtil", "getComicEsBroadcastStream",
//                                        BaseMonitorMessage.METHOD_ERROR.getName());
//                            }
//                            runCount.getAndAdd(1L);
//                            log.info("FlinkUtil getComicEsBroadcastStream task runCount={}", runCount.get());
//                            TimeUnit.MINUTES.sleep(60L);
//                        }
//                    }
//
//                    @Override
//                    public void cancel() {
//                        isRunning = false;
//                    }
//                })
//                .setParallelism(1)
//                .broadcast(CONFIG_KEYWORDS);
//
//        return comicBroadcastStream;
//
//    }
//
//    /**
//     * @note flink消费Kafka多topic初始化
//     * @param kafKaConfig Kafka集群地址
//     * @param kafkaTopicList Kafka topic集合
//     * @param kafkaGroup 消费组
//     * @param flinkRestartCount 重试次数
//     * @param flinkRestartTime 每次重试时间间隔
//     * @param flinkRestartTimeUnit TimeUnit
//     * @return FlinkInitInfo 包含env和多topic中的数据集合
//     */
//    public static FlinkInitInfo getFlinkKafkaListInitInfo(KafKaConfigEnum kafKaConfig, List<String> kafkaTopicList,
//            String kafkaGroup, int flinkRestartCount, long flinkRestartTime, TimeUnit flinkRestartTimeUnit) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //尝试重启次数 每次间隔
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(flinkRestartCount,
//                Time.of(flinkRestartTime, flinkRestartTimeUnit)));
//        Properties properties = KafkaUtil.getConsumeProperties(kafKaConfig, kafkaGroup);
//        //对不同topic的数据collect到list中
//        List<DataStream<String>> messageStreamList = kafkaTopicList.stream().map(config -> {
//
//            FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer(config, new SimpleStringSchema(),
//                    properties);
//            DataStream<String> streamSource = env.addSource(kafkaSource);
//            return streamSource;
//        }).collect(Collectors.toList());
//
//        return FlinkInitInfo.builder().env(env).messageStreamList(messageStreamList).build();
//    }
//
//    public static FlinkInitInfo getFlinkKafkaListInitInfo(List<String> kafkaTopicList, String kafkaGroup,
//            int flinkRestartCount, long flinkRestartTime, TimeUnit flinkRestartTimeUnit) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //尝试重启次数 每次间隔
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(flinkRestartCount,
//                Time.of(flinkRestartTime, flinkRestartTimeUnit)));
//        //对不同topic的数据collect到list中
//        List<DataStream<String>> messageStreamList = kafkaTopicList.stream().map(config -> {
//            KafKaConfigEnum kafKaConfig = TopicAndApiEnum.findKafkaConfigByTopic(config);
//            log.warn("log warn getFlinkKafkaListInitInfo kafKaConfig is {}",kafKaConfig);
//            Properties properties = KafkaUtil.getConsumeProperties(kafKaConfig, kafkaGroup);
//
//            FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer(config, new SimpleStringSchema(),
//                    properties);
//            DataStream<String> streamSource = env.addSource(kafkaSource);
//            return streamSource;
//        }).collect(Collectors.toList());
//
//        return FlinkInitInfo.builder().env(env).messageStreamList(messageStreamList).build();
//    }
//
//    public static FlinkInitInfo getFlinkRocketMqInitInfo(FlinkStartConfig flinkStartConfig) {
//        StreamExecutionEnvironment env = getEnv(flinkStartConfig);
//        RocketMQSource<String> rocketMqSource = RocketMqUtil.getRocketMQSource(flinkStartConfig.getRocketMqConfig(),
//                flinkStartConfig.getMessageTopic(), flinkStartConfig.getMessageGroup());
//        DataStream<String> messageStream = env.addSource(rocketMqSource);
//        return FlinkInitInfo.builder().env(env).messageStream(messageStream).build();
//    }
//
//    public static FlinkInitInfo getFlinkRocketMqInitInfoWithComicEsBroadcastStream(FlinkStartConfig flinkStartConfig,
//            BroadcastProcessFunction<String, List<ComicBean>, String> broadcastProcessFunction) {
//        FlinkInitInfo flinkInitInfo = getFlinkRocketMqInitInfo(flinkStartConfig);
//        StreamExecutionEnvironment env = flinkInitInfo.getEnv();
//        DataStream<String> messageStream = flinkInitInfo.getMessageStream();
//        BroadcastStream<List<ComicBean>> comicBroadcastStream = getComicEsBroadcastStream(env);
//        messageStream.connect(comicBroadcastStream).process(broadcastProcessFunction).print();
//        return flinkInitInfo;
//    }
//}
