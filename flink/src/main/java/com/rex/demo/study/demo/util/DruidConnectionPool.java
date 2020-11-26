package com.rex.demo.study.demo.util;

import com.alibaba.druid.pool.DruidDataSourceFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @Author li zhiqang
 * @create 2020/11/26
 */
public class DruidConnectionPool {

    private transient static DataSource dataSource = null;
    private transient static Properties props = new Properties();

    // 静态代码块
    static {
        props.put("driverClassName", "com.mysql.jdbc.Driver");
        props.put("url", "jdbc:mysql://172.26.55.109:3306/dc?autoReconnect=true&autoReconnectForPools=true&useUnicode=true&characterEncoding=utf8&useSSL=false&useAffectedRows=true&allowMultiQueries=true");
        props.put("username", "root");
        props.put("password", "t4*9&/y?c,h.e17!");
        try {
            dataSource = DruidDataSourceFactory.createDataSource(props);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private DruidConnectionPool() {
    }

    public static Connection getConnection() throws SQLException {
        return dataSource.getConnection();
    }

}
