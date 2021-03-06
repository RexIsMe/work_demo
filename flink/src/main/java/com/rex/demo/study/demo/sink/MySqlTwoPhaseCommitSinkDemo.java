package com.rex.demo.study.demo.sink;

import com.alibaba.fastjson.JSON;
import com.rex.demo.study.demo.entity.Student;
import com.rex.demo.study.demo.util.DruidConnectionPool;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * mysql 两阶段提交sink，保证exactly once
 * 通过ConnectionState将connection对象包装起来就没有报错（相较于{@link MySqlTwoPhaseCommitSink}.）
 *
 * @Author li zhiqang
 * @create 2020/11/26
 */
@Slf4j
public class MySqlTwoPhaseCommitSinkDemo extends TwoPhaseCommitSinkFunction<Tuple2<String, Integer>,
        MySqlTwoPhaseCommitSinkDemo.ConnectionState, Void> {

    // 定义可用的构造函数
    public MySqlTwoPhaseCommitSinkDemo() {
        super(new KryoSerializer<>(ConnectionState.class, new ExecutionConfig()),
                VoidSerializer.INSTANCE);
    }

    @Override
    protected ConnectionState beginTransaction() throws Exception {
        System.out.println("=====> beginTransaction... ");
        //使用连接池，不使用单个连接
//        Class.forName("com.mysql.jdbc.Driver");
//        String url = "jdbc:mysql://172.26.55.109:3306/dc?characterEncoding=utf8&useSSL=false";
//        Connection connection = DBConnectUtil.getConnection(url, "root", "t4*9&/y?c,h.e17!");
        // 使用druid连接池
        Connection connection = DruidConnectionPool.getConnection();
        connection.setAutoCommit(false);//设定不自动提交
        return new ConnectionState(connection);
    }


    @Override
    protected void invoke(ConnectionState transaction, Tuple2<String, Integer> value, Context context) throws Exception {
        Connection connection = transaction.connection;
        /* wordcount demo */
//        PreparedStatement pstm = connection.prepareStatement("INSERT INTO word_count (word, count) VALUES (?, ?) ON DUPLICATE KEY UPDATE count = ?");
//        pstm.setString(1, value.f0);
//        pstm.setInt(2, value.f1);
//        pstm.setInt(3, value.f1);
//        pstm.executeUpdate();

        /* student demo */
        String stu = value.f0;
        Student student = JSON.parseObject(stu, Student.class);
        String sql = "insert into student(id,name,password,age) values (?,?,?,?)";
        PreparedStatement pstm = connection.prepareStatement(sql);
        pstm.setInt(1, student.getId());
        pstm.setString(2, student.getName());
        pstm.setString(3, student.getPassword());
        pstm.setInt(4, student.getAge());
        pstm.execute();
        pstm.close();
    }


    // 先不做处理
    @Override
    protected void preCommit(ConnectionState transaction) throws Exception {
        System.out.println("=====> preCommit... " + transaction);
    }

    //提交事务
    @Override
    protected void commit(ConnectionState transaction) {
        System.out.println("=====> commit... ");
        Connection connection = transaction.connection;
        try {
            if(connection != null){
                connection.commit();
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("提交事物异常");
//            log.error("提交事物异常", e);
        }
    }

    //回滚事务
    @Override
    protected void abort(ConnectionState transaction) {
        System.out.println("=====> abort... ");
        Connection connection = transaction.connection;
        try {
            if(connection != null){
                connection.rollback();
                connection.close();
            }
        } catch (SQLException e) {
            throw new RuntimeException("回滚事物异常");
//            log.error("回滚事物异常", e);
        }
    }

    //定义建立数据库连接的方法
    public static class ConnectionState {
        private final transient Connection connection;

        public ConnectionState(Connection connection) {
            this.connection = connection;
        }
    }
}

