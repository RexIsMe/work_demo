package com.rex.demo.study.demo.asyncrequest;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Supplier;

/**
 * TODO Flink 异步查询 MySQL 数据
 *
 * @author liuzebiao
 * @Date 2020-2-14 17:16
 */
public class AsyncMySQLRequest extends RichAsyncFunction<String,String> {

    private transient DruidDataSource dataSource;

    private transient ExecutorService executorService;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        executorService = Executors.newFixedThreadPool(20);

        dataSource = new DruidDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
        dataSource.setUrl("jdbc:mysql://localhost:3306/table_name?characterEncoding=UTF-8");
        dataSource.setInitialSize(5);
        dataSource.setMinIdle(10);
        dataSource.setMaxActive(20);
    }

    @Override
    public void asyncInvoke(String id, ResultFuture<String> resultFuture) throws Exception {

        Future<String> future = executorService.submit(() -> {
            return queryFromMySql(id);
        });

        CompletableFuture.supplyAsync(new Supplier<String>() {

            @Override
            public String get() {
                try {
                    return future.get();
                } catch (Exception e) {
                    return null;
                }
            }
        }).thenAccept((String dbResult) ->{
            resultFuture.complete(Collections.singleton(dbResult));
        });
    }

    private String queryFromMySql(String param) throws SQLException {
        String sql = "SELECT id,name FROM t_table WHERE id = ?";
        String result = null;

        Connection connection = null;
        PreparedStatement stmt = null;
        ResultSet rs = null;

        try {
            connection = dataSource.getConnection();
            stmt = connection.prepareStatement(sql);
            stmt.setString(1, param);
            rs = stmt.executeQuery();
            while(rs.next()){
                result = rs.getString("name");
            }
        } finally {
            if (rs != null) {
                rs.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        if(result != null){
            //放入缓存中
        }
        return result;
    }

    @Override
    public void close() throws Exception {
        dataSource.close();
        executorService.shutdown();
    }
}


