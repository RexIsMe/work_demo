package com.rex.demo.study.demo.driver.state;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.rex.demo.study.demo.util.FlinkUtils2;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.UUID;

/**
 * TODO Broadcast State 广播状态 Demo
 *
 * @author liuzebiao
 * @Date 2020-2-25 13:45
 */
public class BroadcastStateDemo {

    public static void main(String[] args) throws Exception {

        //使用ParameterTool,读取配置文件信息
        ParameterTool parameters = ParameterTool.fromPropertiesFile("配置文件路径");

        /********************************** 获取 A流过程 start *************************************/
        //读取 Kafka 中要处理的数据
        //(100001,A1,2020-02-12)
        DataStream<String> kafkaStream = FlinkUtils2.createKafkaStream(parameters, "topicA", "groupA", SimpleStringSchema.class);

        //对数据进行处理后，得到 A 流
        SingleOutputStreamOperator<Tuple3<String, String, String>> streamA = kafkaStream.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });
        /********************************** 获取 A流过程 end *************************************/


        /********************************** 获取 B流过程 start *************************************/
        //读取 经 Canal 发送到 Kafka 指定 topic 中的数据
        //(A1,普通会员)
        DataStream<String> dicDataStream = FlinkUtils2.createKafkaStream(parameters, "user_type_dic", UUID.randomUUID().toString(), SimpleStringSchema.class);

        SingleOutputStreamOperator<Tuple3<String, String, String>> typeStream = dicDataStream.process(new ProcessFunction<String, Tuple3<String, String, String>>() {
            @Override
            public void processElement(String jsonStr, Context context, Collector<Tuple3<String, String, String>> out) throws Exception {
                //Canal 发送到 Kafka 的消息为 JSON 格式，接下来完成对 Json 格式的拆分
                JSONObject jsonObject = JSON.parseObject(jsonStr);
                JSONArray jsonArray = jsonObject.getJSONArray("data");

                String type = jsonObject.getString("type");
                if ("INSERT".equals(type) || "UPDATE".equals(type) || "DELETE".equals(type)) {
                    for (int i = 0; i < jsonArray.size(); i++) {
                        JSONObject obj = jsonArray.getJSONObject(i);
                        String id = obj.getString("id");
                        String name = obj.getString("name");
                        out.collect(Tuple3.of(id, name, type));
                    }
                }
            }
        });
        /********************************** 获取 B流过程 end *************************************/

        /********************************** 将 B 流进行广播 start *************************************/
        /**定义一个广播的状态描述器**/
        MapStateDescriptor<String, String> mapStateDescriptor = new MapStateDescriptor<String, String>(
                "dic-state",
                Types.STRING,
                Types.STRING
        );
        /**将 B 流进行广播，得到 BroadcastStream 类型的广播数据流**/
        BroadcastStream<Tuple3<String, String, String>> broadcastStateStreamB = typeStream.broadcast(mapStateDescriptor);
        /********************************** 将 B 流进行广播 end *************************************/


        /********************************** 将 A 流 与广播出去的 B流 进行关联 start *************************************/
        /**
         * 将要处理的流 A 与 已广播的流 B 进行关联操作
         */
        SingleOutputStreamOperator<Tuple4<String, String, String, String>> broadcastStream = streamA.connect(broadcastStateStreamB).process(new BroadcastProcessFunction<Tuple3<String, String, String>, Tuple3<String, String, String>, Tuple4<String, String, String, String>>() {
            /**
             * 处理要计算的活动数据
             * @param tuple A流中的数据
             * @param readOnlyContext 只读上下文(只负责读，不负责写)
             * @param out 返回的数据
             * @throws Exception
             */
            @Override
            public void processElement(Tuple3<String, String, String> tuple, ReadOnlyContext readOnlyContext, Collector<Tuple4<String, String, String, String>> out) throws Exception {
                ReadOnlyBroadcastState<String, String> mapState = readOnlyContext.getBroadcastState(mapStateDescriptor);

                String id = tuple.f0;
                String type = tuple.f1;
                String time = tuple.f2;

                //根据用户类型ID，到广播的stateMap中关联对应的数据
                String typeName = mapState.get(type);

                //将关联后的数据，进行输出
                out.collect(Tuple4.of(id, type, typeName, time));
            }

            /**
             * 处理规则数据
             * @param tuple 广播流中的数据
             * @param context 上下文(此处负责写)
             * @param out
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Tuple3<String, String, String> tuple, Context context, Collector<Tuple4<String, String, String, String>> out) throws Exception {

                //id
                String id = tuple.f0;
                String name = tuple.f1;
                String type = tuple.f2;

                //新来一条规则数据，就将规则数据添加到内存
                BroadcastState<String, String> mapState = context.getBroadcastState(mapStateDescriptor);

                if ("DELETE".equals(type)) {
                    mapState.remove(id);
                } else {
                    mapState.put(id, name);
                }

            }
        });
        /********************************** 将 A 流 与广播出去的 B流 进行关联 end *************************************/

        //输出最终返回的流
        //(100001,A1,普通会员,2020-02-12)
        //(100002,A2,金牌会员,2020-02-05)
        //(100003,A3,钻石会员,2020-01-09)
        broadcastStream.print();

        FlinkUtils2.getEnv().execute("BroadcastStateDemo");

    }
}

