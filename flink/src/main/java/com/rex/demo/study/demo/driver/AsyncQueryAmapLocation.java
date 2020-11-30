package com.rex.demo.study.demo.driver;

import com.rex.demo.study.demo.asyncrequest.AsyncDatabaseRequest;
import com.rex.demo.study.demo.entity.DataLocation;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * TODO 读取 Kafka 数据,异步调用高德API,将经纬度转换成省份输出
 * <p>
 * log日志格式：100001,Lucy,2020-02-14 13:28:27,116.310003,39.991957
 * <p>
 * 高德API:
 * https://restapi.amap.com/v3/geocode/regeo?output=json&location=116.310003,39.991957&key=<你申请的key></>&radius=1000&extensions=all
 *
 * @author liuzebiao
 * @Date 2020-2-14 13:22
 */
public class AsyncQueryAmapLocation {

    public static void main(String[] args) throws Exception {
        /**1.创建流实时环境**/
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /**2.读取 KafkaSource 数据**/
        //Kafka props
        Properties properties = new Properties();
        //指定Kafka的Broker地址
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.26.55.116:9092");
        //指定组ID
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "flinkDemoGroup");
        //如果没有记录偏移量，第一次从最开始消费
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        FlinkKafkaConsumer011<String> kafkaConsumer011 = new FlinkKafkaConsumer011<>("locations", new SimpleStringSchema(), properties);
        kafkaConsumer011.setStartFromLatest();
        //2.通过addSource()方式，创建 Kafka DataStream
        DataStreamSource<String> kafkaDataStream = env.addSource(kafkaConsumer011);

        /**3.异步处理数据**/
        /**此处使用 unorderedWait()无序等待,即：发起请求的顺序和结果返回的顺序可能不一样**/
        /**你也可以使用 orderedWait()有序等待,即：发起请求的顺序和结果返回的顺序一致，这个相对于无序来说，性能可能就有一定的损耗了**/
        // apply the async I/O transformation
        DataStream<DataLocation> resultStream =
                AsyncDataStream.unorderedWait(kafkaDataStream, new AsyncDatabaseRequest(), 0, TimeUnit.MILLISECONDS, 10);//10代表最多允许有10个异步请求，超过10个就会阻塞
//0代表超时时间。0表示不设置超时时间

        resultStream.print();

        env.execute("AsyncQueryAmapLocation");
    }
}

