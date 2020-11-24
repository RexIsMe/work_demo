//package com.rex.demo.study.kuaikan;
//
//import com.alibaba.fastjson.JSONObject;
//import com.kuaikan.data.flink.common.flink.FlinkInitInfo;
//import com.kuaikan.data.flink.common.flink.FlinkUtil;
//import com.kuaikan.data.flink.common.kafka.KafkaUtil.KafKaConfigEnum;
//import com.kuaikan.data.flink.common.kafka.KafkaUtil.KafkaTopicConfig.BigData;
//import com.kuaikan.data.flink.common.redis.RedisCacheConfig;
//import com.kuaikan.data.flink.common.redis.RedisKeyGenerator;
//import com.kuaikan.data.flink.common.redis.RedisUtil;
//import com.kuaikan.data.flink.common.redis.RedisUtil.RedisClusterEnum;
//import lombok.Data;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//
//import java.io.Serializable;
//import java.util.Map;
//
///**
// * @author weizhiyu
// * @create 2019-07-17 12:03
// */
//@Slf4j
//public class TopicFavouriteFlink {
//
//    private static final String TOPIC_FAVOURITE_KAFKA_GROUP_ID = "topic_favourite_flink";
//
//    public static void main(String[] args) throws Exception {
//        log.info("PostViewFlink task start");
//        FlinkInitInfo flinkKafkaInitInfo = FlinkUtil.getFlinkKafkaInitInfo(FlinkUtil.FlinkStartConfig.builder()
//                .kafKaConfig(KafKaConfigEnum.BIG_DATA)
//                .messageTopic(BigData.KK_TOPIC_FAV)
//                .messageGroup(TOPIC_FAVOURITE_KAFKA_GROUP_ID)
////                .flinkCheckpointConfig(FlinkUtil.FlinkCheckpointConfig.builder().build())
//                .build());
//        StreamExecutionEnvironment env = flinkKafkaInitInfo.getEnv();
//        DataStream<String> messageStream = flinkKafkaInitInfo.getMessageStream();
//        messageStream.rebalance().map(new TopicFavouriteMapFunction()).addSink(new TopicFavouriteRichSinkFunction());
//        env.execute("data_flink_flink-task_TopicFavouriteFlink_wzy");
//
//    }
//
//    static class TopicFavouriteMapFunction implements MapFunction<String, TopicFavourite> {
//
//        @Override
//        public TopicFavourite map(String value) {
//            if (StringUtils.isBlank(value)) {
//                return null;
//            }
//            try {
//                return JSONObject.parseObject(value, TopicFavourite.class);
//            } catch (Exception e) {
//                log.error("TopicFavouriteFlink TopicFavouriteMapFunction map error value={}", value, e);
//            }
//            return null;
//        }
//    }
//
//    static class TopicFavouriteRichSinkFunction extends RichSinkFunction<TopicFavourite> {
//
//        private static final int TOPIC_FAVORITE_VALID_STATUS = 1;
//
//        @Override
//        public void invoke(TopicFavourite topicFavourite, Context context) {
//            try {
//                int userId = topicFavourite.getUserId();
//                int status = topicFavourite.getStatus();
//                long favouriteTime = topicFavourite.getFavouriteTime();
//                String topicId = String.valueOf(topicFavourite.getTargetId());
//                String userProfileOnlineCacheKey = RedisKeyGenerator
//                        .generate(RedisCacheConfig.USER_PROFILE_ONLINE_CACHE_KEY.getKeyPattern(), userId);
//                String realTimeFavMapKey = RedisCacheConfig.USER_PROFILE_REAL_TIME_FAV_MAP_KEY.getKeyPattern();
//                Map<String, Long> realTimeFavMap = RedisUtil.getUserProfileMapValueMap(
//                        RedisUtil.getRedisCluster(RedisClusterEnum.RECOMMEND_ONLINE), userProfileOnlineCacheKey,
//                        realTimeFavMapKey);
//
//                if (status == TOPIC_FAVORITE_VALID_STATUS) {
//                    realTimeFavMap.put(topicId, favouriteTime);
//                } else {
//                    realTimeFavMap.remove(topicId);
//                }
//                Map<String, String> updateUserProfileMap = RedisUtil.updateUserProfile(String.valueOf(userId),
//                        realTimeFavMapKey, 2, realTimeFavMap);
//                //                log.info(
//                //                        "PostViewFlink TopicFavouriteFlink invoke update redis success userId={} updateUserProfileMap={}",
//                //                        userId, updateUserProfileMap);
//            } catch (Exception e) {
//                log.error("TopicFavouriteFlink TopicFavouriteFlink invoke error topicFavourite={}", topicFavourite, e);
//            }
//
//        }
//    }
//
//    @Data
//    public static class TopicFavourite implements Serializable {
//
//        private static final long serialVersionUID = -5998049891936665688L;
//
//        private long targetId;
//        private int userId;
//        private int status;
//        private long favouriteTime;
//    }
//
//}
