package com.rex.demo.study.demo.sink;

import com.alibaba.fastjson.JSON;
import com.rex.demo.study.demo.entity.Student;
import com.rex.demo.study.demo.util.DBConnectUtil;
import com.rex.demo.study.demo.util.DruidConnectionPool;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.base.VoidSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.JavaSerializer;
import org.apache.flink.api.java.typeutils.runtime.kryo.KryoSerializer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 *
 *
 * @Author li zhiqang
 * @create 2020/11/26
 */
public class MySqlTwoPhaseCommitSink extends TwoPhaseCommitSinkFunction<ObjectNode, Connection, Void> {

    public MySqlTwoPhaseCommitSink() {
        super(new KryoSerializer<>(Connection.class, new ExecutionConfig()), VoidSerializer.INSTANCE);
//        super(new JavaSerializer<ObjectNode>(), VoidSerializer.INSTANCE);
    }

    /**
     * 执行数据入库操作
     * @param connection
     * @param objectNode
     * @param context
     * @throws Exception
     */
    @Override
    protected void invoke(Connection connection, ObjectNode objectNode, SinkFunction.Context context) throws Exception {
        System.err.println("start invoke.......");
        String stu = objectNode.get("value").toString();
        Student student = JSON.parseObject(stu, Student.class);
        String sql = "insert into student(id,name,password,age) values (?,?,?,?)";
        PreparedStatement ps = connection.prepareStatement(sql);
        ps.setInt(1, student.getId());
        ps.setString(2, student.getName());
        ps.setString(3, student.getPassword());
        ps.setInt(4, student.getAge());
//        ps.executeUpdate();
        ps.execute();
    }

    /**
     * 获取连接，开启手动提交事物（getConnection方法中）
     * @return
     * @throws Exception
     */
    @Override
    protected Connection beginTransaction() throws Exception {
        //使用单个连接
//        String url = "jdbc:mysql://172.26.55.109:3306/dc?characterEncoding=utf8&useSSL=false";
//        Connection connection = DBConnectUtil.getConnection(url, "root", "t4*9&/y?c,h.e17!");
        //使用连接池，不使用单个连接
        Connection connection = DruidConnectionPool.getConnection();
        connection.setAutoCommit(false);//关闭自动提交
        System.err.println("start beginTransaction......."+connection);
        return connection;
    }

    /**
     * 预提交，这里预提交的逻辑在invoke方法中
     * @param connection
     * @throws Exception
     */
    @Override
    protected void preCommit(Connection connection) throws Exception {
        System.err.println("start preCommit......."+connection);

    }

    /**
     * 如果invoke执行正常则提交事物
     * @param connection
     */
    @Override
    protected void commit(Connection connection) {
        System.err.println("start commit......."+connection);
        DBConnectUtil.commit(connection);

    }

    @Override
    protected void recoverAndCommit(Connection connection) {
        System.err.println("start recoverAndCommit......."+connection);

    }


    @Override
    protected void recoverAndAbort(Connection connection) {
        System.err.println("start abort recoverAndAbort......."+connection);
    }

    /**
     * 如果invoke执行异常则回滚事物，下一次的checkpoint操作也不会执行
     * @param connection
     */
    @Override
    protected void abort(Connection connection) {
        System.err.println("start abort rollback......."+connection);
        DBConnectUtil.rollback(connection);
    }

}
