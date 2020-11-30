package com.rex.demo.study.demo.asyncrequest;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.rex.demo.study.demo.entity.DataLocation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.http.HttpResponse;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
import org.apache.http.impl.nio.client.HttpAsyncClients;
import org.apache.http.util.EntityUtils;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * TODO 发起 读取数据库/三方API 异步请求
 * 备注：此部分也可以在上面一段代码中编写,因为重写的方法太多,显得混乱,所以在此处可以单独写一个类出来
 * @author liuzebiao
 * @Date 2020-2-14 13:40
 */
public class AsyncDatabaseRequest extends RichAsyncFunction<String, DataLocation> {

    private transient CloseableHttpAsyncClient httpAsyncClient = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        //初始化异步的HttpClient
        RequestConfig requestConfig = RequestConfig.custom()
                .setSocketTimeout(3000) //设置Socket超时时间
                .setConnectTimeout(3000) //设置连接超时时间
                .build();

        httpAsyncClient = HttpAsyncClients.custom()
                .setMaxConnTotal(20)//设置最大连接数
                .setDefaultRequestConfig(requestConfig).build();

        httpAsyncClient.start();

    }

    /**
     * 完成对 Kafka 中读取到的数据的操作
     * @param line
     * @param resultFuture
     * @throws Exception
     */
    @Override
    public void asyncInvoke(String line, ResultFuture<DataLocation> resultFuture) throws Exception {
//        100001,Lucy,2020-02-14 13:28:27,116.310003,39.991957
        String[] fields = line.split(",");
        //id
        String id = fields[0];
        //name
        String name = fields[1];
        //date
        String date = fields[2];
        //longitude
        String longitude = fields[3];
        //latitude
        String latitude = fields[4];

//        String amap_url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+longitude+","+latitude+"&key=<你申请的key>&radius=1000&extensions=all";
        String amap_url = "https://restapi.amap.com/v3/geocode/regeo?output=json&location="+longitude+","+latitude+"&key=3feee53d5be6114549e180b5a287af81&radius=1000&extensions=all";

        // Execute request
        final HttpGet request1 = new HttpGet(amap_url);
        Future<HttpResponse> future = httpAsyncClient.execute(request1, null);

        // set the callback to be executed once the request by the client is complete
        // the callback simply forwards the result to the result future
        CompletableFuture.supplyAsync(new Supplier<String>() {
            @Override
            public String get() {
                try {
                    HttpResponse response = future.get();
                    String province = null;
                    if (response.getStatusLine().getStatusCode() == 200) {
                        //获取请求的Json字符串
                        String result = EntityUtils.toString(response.getEntity());
                        //将Json字符串转成Json对象
                        JSONObject jsonObject = JSON.parseObject(result);
                        //获取位置信息
                        JSONObject regeocode = jsonObject.getJSONObject("regeocode");

                        if (regeocode != null && !regeocode.isEmpty()) {
                            JSONObject address = regeocode.getJSONObject("addressComponent");
                            //获取省份信息
                            province = address.getString("province");
                        }
                    }
                    return province;
                } catch (Exception e) {
                    // Normally handled explicitly.
                    return null;
                }
            }
        }).thenAccept( (String dbResult) -> {
            resultFuture.complete(Collections.singleton(DataLocation.of(id,name,date,dbResult)));
        });

    }

    @Override
    public void close() throws Exception {
        super.close();
        httpAsyncClient.close();
    }
}


