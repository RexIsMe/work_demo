package com.rex.demo.study.demo.util;

import com.alibaba.fastjson.JSON;
import com.rex.demo.study.demo.entity.DataLocation;
import com.rex.demo.study.demo.entity.Student;
import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 CREATE TABLE `student` (
 `id` int(11) unsigned NOT NULL,
 `name` varchar(25) COLLATE utf8_bin DEFAULT NULL,
 `password` varchar(25) COLLATE utf8_bin DEFAULT NULL,
 `age` int(10) DEFAULT NULL
 ) ENGINE=InnoDB DEFAULT CHARSET=utf8 COLLATE=utf8_bin
 */
public class KafkaProducerUtil {

    public static final String brokerServers = "172.26.55.116:9092";

    public static final String topic = "student";

    public static void main(String[] args) {
//        sendMessage();

        List lists = new ArrayList();
        lists.add("100001,Lucy,2020-02-14 13:28:27,116.310003,39.991957");
        lists.add("100002,Lucy,2020-02-14 13:28:27,118.980206,36.387564");
        lists.add("100003,Lucy,2020-02-14 13:28:27,104.236553,33.120868");
        lists.add("100004,Lucy,2020-02-14 13:28:27,103.05003,39.21682");
        lists.add("100005,Lucy,2020-02-14 13:28:27,112.124737,38.566952");
        lists.add("100006,Lucy,2020-02-14 13:28:27,106.565655,29.269022");
        lists.add("100007,Lucy,2020-02-14 13:28:27,121.089581,23.96805");
        lists.add("100008,Lucy,2020-02-14 13:28:27,114.162701,22.433236");
        sendMessage("locations", lists, null);
    }

    public static KafkaProducer getDefaultKafkaProducer(){
        Properties prop = new Properties();
        prop.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerServers);
        prop.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        prop.put(ProducerConfig.ACKS_CONFIG, "all");
        prop.put(ProducerConfig.BATCH_SIZE_CONFIG, "10000");
        prop.put(ProducerConfig.RETRIES_CONFIG, 0);
        KafkaProducer producer = new KafkaProducer(prop);
        return producer;
    }

    private static void sendMessage() {
        KafkaProducer producer = getDefaultKafkaProducer();

        //循环发送
        for (int i = 1; i <= 100; i++) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            Student student = new Student(i, "stu_" + i, "123456", 10 + i);
            ProducerRecord<String,String> record = new ProducerRecord<>(topic, JSON.toJSONString(student));
            defaultSend(producer, record);
        }
        producer.flush();
        producer.close();
    }

    /**
     * 向指定topic发送数据
     * @param topic
     * @param strlists
     * @param <T>
     */
    private static <T> void sendMessage(String topic, List<String> strlists, Long sleepTime) {
        KafkaProducer producer = getDefaultKafkaProducer();
        int size = strlists.size();
        //循环发送
        for (int i = 0; i < size; i++) {
            if(sleepTime != null){
                try {
                    TimeUnit.MILLISECONDS.sleep(sleepTime);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            ProducerRecord<String,String> record = new ProducerRecord<>(topic, strlists.get(i));
            defaultSend(producer, record);
        }
        producer.flush();
        producer.close();
    }

    public static void defaultSend(KafkaProducer producer, ProducerRecord<String,String> record){
        producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) exception.printStackTrace();
                String data = "message send to => topic: " + metadata.topic() + ",partition: " + metadata.partition() + ",offset: " + metadata.offset()+",value: "+ record.value();
                System.err.println(data);
            }
        });
    }

}
