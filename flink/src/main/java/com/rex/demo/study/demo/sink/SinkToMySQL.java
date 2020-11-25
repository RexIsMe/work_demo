package com.rex.demo.study.demo.sink;


import com.alibaba.fastjson.JSONObject;
import com.rex.demo.study.demo.entity.DCRecordEntity;
import com.rex.demo.study.demo.util.Md5Utils;
import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;

/**
 * 程序功能: 数据批量 sink 数据到 mysql
 *
 * @Author li zhiqang
 * @create 2020/11/25
 */
public class SinkToMySQL extends RichSinkFunction<List<String>> {
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
        connection.setAutoCommit(false);
        String sql = "insert into dc.part_stock_realtime(md5_key, organization_id, sum_inventory_number) values(?,?,?);";
        ps = this.connection.prepareStatement(sql);
    }

    @Override
    public void close() throws Exception {
        if (connection != null) {
            connection.commit();
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
        for (String str : value) {
            DCRecordEntity dcRecordEntity = JSONObject.parseObject(str, DCRecordEntity.class);
            ps.setString(1, Md5Utils.stringToMD5(dcRecordEntity.getTimestamp().toString()));
            ps.setLong(2, dcRecordEntity.getDataList().get(0).getLong("organizationId"));
            ps.setInt(3, dcRecordEntity.getDataList().get(0).getInteger("inventoryNumber"));
            ps.addBatch();
        }
        int[] count = ps.executeBatch();  //批量后执行
        System.out.println("成功了插入了" + count.length + "行数据");
    }


    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://172.26.55.109:3306/dc?autoReconnect=true&autoReconnectForPools=true&useUnicode=true&characterEncoding=utf8&useSSL=false&useAffectedRows=true&allowMultiQueries=true");
        dataSource.setUsername("root");
        dataSource.setPassword("t4*9&/y?c,h.e17!");
        //设置连接池的一些参数
        dataSource.setInitialSize(10);
        dataSource.setMaxTotal(50);
        dataSource.setMinIdle(2);

        Connection con = null;
        try {
            con = dataSource.getConnection();
            System.out.println("创建连接池：" + con);
        } catch (Exception e) {
            System.out.println("-----------mysql get connection has exception , msg = " + e.getMessage());
        }
        return con;
    }
}

