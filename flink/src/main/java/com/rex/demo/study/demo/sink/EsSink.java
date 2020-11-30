package com.rex.demo.study.demo.sink;

import com.rex.demo.study.demo.constants.FlinkConstants;
import com.rex.demo.study.demo.util.PropertiesUtils;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.client.Requests;

import java.util.*;
import java.util.stream.Collectors;

/**
 * TODO ElasticSearch Sink
 *
 * 使用示例：
 * public static void main(String[] args) throws Exception {
 * 	//处理数据完成，返回Tuple4<String,String,String,Map<String,Object>>类型数据
 * 	//buildUVSinkStream() 方法为处理过程
 * 	SingleOutputStreamOperator<Tuple4<String,String,String,Map<String,Object>>> uvSinkStream = buildUVSinkStream(dataStream);
 * 	uvSinkStream.addSink(EsSink.sinkToEs()).name("xxx");
 * }
 * @author liuzebiao
 * @Date 2020-4-24 18:46
 */
@Slf4j
public class EsSink{

    /**
     * 获取ElasticSearch地址、端口号(此处定义为常量,通过 PropertieUtil.getValue()方式来获取)
     */
    private static String elasticSearchHosts = PropertiesUtils.getProperty(FlinkConstants.ELASTICSEARCH_HOSTS);
    private static String port = PropertiesUtils.getProperty(FlinkConstants.ELASTICSEARCH_PORT);

    /**
     * sinkToEs 实现
     */
    public static ElasticsearchSink<Tuple4<String,String,String,Map<String,Object>>> sinkToEs() {

        /**
         * 创建一个ElasticSearchSink对象
         */
        List<HttpHost> httpHost = Arrays.stream(elasticSearchHosts.split(",")).map(host -> new HttpHost(host, Integer.parseInt(port), "http")).collect(Collectors.toList());

        ElasticsearchSink.Builder<Tuple4<String, String, String, Map<String, Object>>> esSinkBuilder = new ElasticsearchSink.Builder<>(httpHost, (Tuple4<String, String, String, Map<String, Object>> tp4, RuntimeContext ctx, RequestIndexer indexer) -> {
            // 数据保存在Elasticsearch中,返回【index,type,id,数据】组成的Tuple4
            //index
            String indexName = tp4.f0;
            //type
            String typeName = tp4.f1;
            //id
            String idx = tp4.f2;
            //source
            Map<String, Object> sources = tp4.f3;

            // MapUtils 工具类，来自:org.apache.commons.collections 包。maven导入即可
            if (MapUtils.isNotEmpty(sources)) {
                try {
                    indexer.add(Requests.indexRequest().index(indexName).type(typeName).id(idx).source(sources));
                } catch (Exception e) {
                    log.error("保存es报错,sources:" + sources, e);
                }
            }
        });
        // 设置批量写数据的缓冲区大小
        esSinkBuilder.setBulkFlushMaxActions(1);
        return esSinkBuilder.build();
    }
}

