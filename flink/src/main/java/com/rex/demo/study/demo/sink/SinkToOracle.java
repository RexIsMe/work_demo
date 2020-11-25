package com.rex.demo.study.demo.sink;


import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.List;


/**
 * 程序功能: 数据批量 sink 数据到 oracle
 *
 * @Author li zhiqang
 * @create 2020/11/25
 */
public class SinkToOracle extends RichSinkFunction<List<String>> {
    private PreparedStatement ps;
    private BasicDataSource dataSource;
    private Connection connection;

    /**
     * open() 方法中建立连接，这样不用每次 invoke 的时候都要建立连接和释放连接
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        dataSource = new BasicDataSource();
        connection = getConnection(dataSource);
        String sql = "insert into STUDENT(ID,NAME,PASSWORD,AAAA) values(?, ?, ?, ?)";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.close();
        }
        if (ps != null) {
            ps.close();
        }
    }

    /**
     * 每批数据的插入都要调用一次 invoke() 方法
     */
    @Override
    public void invoke(List<String> value, Context context) throws Exception {
        //遍历数据集合
        for (String student : value) {
            ps.setInt(1, 1);
            ps.setString(2, student);
            ps.setString(3, "123456");
            ps.setInt(4, 18);
            ps.addBatch();
        }
        int[] count = ps.executeBatch();  //批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
    }


    private static Connection getConnection(BasicDataSource dataSource) {
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager.getConnection("jdbc:oracle:thin:@localhost:1521:xe", "hr", "hr");
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}

