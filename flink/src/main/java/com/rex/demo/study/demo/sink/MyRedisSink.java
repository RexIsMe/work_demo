package com.rex.demo.study.demo.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;

/**
 * TODO 自定义 Redis Sink
 *
 * @author liuzebiao
 * @Date 2020-2-18 10:26
 */
public class MyRedisSink extends RichSinkFunction<Tuple3<String, String, Integer>> {

    private transient Jedis jedis;

    @Override
    public void open(Configuration config) {

        ParameterTool parameters = (ParameterTool)getRuntimeContext().getExecutionConfig().getGlobalJobParameters();
        String host = parameters.getRequired("redis.host");
        String password = parameters.get("redis.password", "");
        Integer port = parameters.getInt("redis.port", 6379);
        Integer timeout = parameters.getInt("redis.timeout", 5000);
        Integer db = parameters.getInt("redis.db", 0);
        jedis = new Jedis(host, port, timeout);
        jedis.auth(password);
        jedis.select(db);
    }


    @Override
    public void invoke(Tuple3<String, String, Integer> value, Context context) throws Exception {
        if (!jedis.isConnected()) {
            jedis.connect();
        }
        //保存
        jedis.hset(value.f0, value.f1, String.valueOf(value.f2));
    }

    @Override
    public void close() throws Exception {
        jedis.close();
    }
}

