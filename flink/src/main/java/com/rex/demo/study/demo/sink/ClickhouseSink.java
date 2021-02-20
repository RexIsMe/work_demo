//package com.rex.demo.study.demo.sink;
//
//import org.apache.commons.lang3.StringUtils;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
//import org.apache.flink.types.Row;
//
//import java.io.Serializable;
//import java.sql.*;
//import java.util.*;
//
///**
// * @Author li zhiqang
// * @create 2021/2/20
// */
//public class ClickhouseSink extends RichSinkFunction<Row> implements Serializable {
//    private String tablename;
//    private String[] tableColums;
//    private List<String> types;
//    private String[] columns;
//    private String username;
//    private String password;
//    private String[] ips;
//    private String drivername = "ru.yandex.clickhouse.ClickHouseDriver";
//    private List<Row> list = new ArrayList<>();
//    private List<PreparedStatement> preparedStatementList = new ArrayList<>();
//    private List<Connection> connectionList = new ArrayList<>();
//    private List<Statement> statementList = new ArrayList<>();
//
//    private long lastInsertTime = 0L;
//    private long insertCkTimenterval = 4000L;
//    // 插入的批次
//    private int insertCkBatchSize = 10000;
//
//    public ClickhouseSink(String tablename, String username, String password, String[] ips, String[] tableColums, List<String> types, String[] columns) {
//        this.tablename = tablename;
//        this.username = username;
//        this.password = password;
//        this.ips = ips;
//        this.tableColums = tableColums;
//        this.types = types;
//        this.columns = columns;  // 新增字段
//    }
//
//    // 插入数据
//    public void insertData(List<Row> rows, PreparedStatement preparedStatement, Connection connection) throws SQLException {
//
//        for (int i = 0; i < rows.size(); ++i) {
//            Row row = rows.get(i);
//            for (int j = 0; j < this.tableColums.length; ++j) {
//                if (null != row.getField(j)) {
//                    preparedStatement.setObject(j + 1, row.getField(j));
//
//                } else {
//                    preparedStatement.setObject(j + 1, "null");
//                }
//            }
//            preparedStatement.addBatch();
//        }
//
//        preparedStatement.executeBatch();
//        connection.commit();
//        preparedStatement.clearBatch();
//    }
//
//
//    /**
//     * 新增字段修改表添加列
//     *
//     * @param statement
//     * @throws Exception
//     */
//    public void tableAddColumn(Statement statement) {
//        try {
//            if (null != this.columns && this.columns.length > 0) {
//
//                /**
//                 * table 增加字段
//                 */
//                // 获取原表字段名
//                String querySql = "select * from " + this.tablename + " limit 1";
//
//                ResultSet rs = statement.executeQuery(querySql);
//                ResultSetMetaData rss = rs.getMetaData();
//                int columnCount = rss.getColumnCount();
//
//                List<String> orgTabCols = new ArrayList<>();
//                for (int i = 1; i <= columnCount; ++i) {
//                    orgTabCols.add(rss.getColumnName(i));
//                }
//
//                // 对比两个数组,判断新增字段是否在原来的表中
//                Collection collection = new ArrayList<String>(orgTabCols);
//                boolean exists = collection.removeAll(Arrays.asList(this.columns));
//
//                // 新增字段不在原来的表中，执行添加列操作
//                if (!exists) {
//
//                    for (int i = 0; i < this.columns.length; ++i) {
//                        String str = "";
//                        String str_all = "";
//
//                        StringBuilder sb = null;
//                        StringBuilder sb_all = null;
//                        if (i == 0) {
//                            sb.append("alter table " ).append(this.tablename).append(" add column ").append(this.columns[i]).append(" String").append(" after ").append(orgTabCols.get(orgTabCols.size() - 1));
//                            sb_all.append("alter table " ).append("_all").append(this.tablename).append(" add column ").append(this.columns[i]).append(" String").append(" after ").append(orgTabCols.get(orgTabCols.size() - 1));
//
//                        } else {
//                            sb.append("alter table " ).append(this.tablename).append(" add column ").append(this.columns[i]).append(" String").append(" after ").append(this.columns[i - 1]);
//
//                            sb_all.append("alter table " ).append("_all").append(this.tablename).append(" add column ").append(this.columns[i]).append(" String").append(" after ").append(this.columns[i - 1]);
//                        }
//
//                        if (StringUtils.isNotEmpty(sb.toString())) {
//                            statement.executeUpdate(sb.toString());
//                        }
//
//                        if (StringUtils.isNotEmpty(sb_all.toString())) {
//                            statement.executeUpdate(sb_all.toString());
//                        }
//                    }
//                }
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//        }
//    }
//
//    // 根据IP创建连接
//    public void createConnection() throws Exception {
//
//        // 插入语句
//        String insertStr = StrUtils.clickhouseInsertValue(this.tableColums, this.tablename);
//        // 创建表
//        List<String> createtableStrList = StrUtils.clickhouseCreatTable(this.tableColums, this.tablename, Constant.CKCLUSTERNAME, this.tableColums[3], this.types);
//        // 创建数据库
//        String create_database_str = "create database if not exists " + this.tablename.split("\\.")[0];
//
//        for (String ip : this.ips) {
//            String url = "jdbc:clickhouse://" + ip + ":8123";
//            Connection connection = DriverManager.getConnection(url, this.username, this.password);
//            Statement statement = connection.createStatement();
//
//            // 执行创建数据库
//            statement.executeUpdate(create_database_str);
//
//            // 执行创建表
//            statement.executeUpdate(createtableStrList.get(0));
//            statement.executeUpdate(createtableStrList.get(1));
//
//            // 增加表字段
//            tableAddColumn(statement);
//
//            this.statementList.add(statement);
//            PreparedStatement preparedStatement = connection.prepareStatement(insertStr);
//            connection.setAutoCommit(false);
//            this.preparedStatementList.add(preparedStatement);
//            this.connectionList.add(connection);
//        }
//
//    }
//
//
//    @Override
//    public void open(Configuration parameters) throws Exception {
//
//        Class.forName(this.drivername);
//
//        // 创建连接
//        createConnection();
//    }
//
//    @Override
//    public void invoke(Row row, Context context) throws Exception {
//
//        // 轮询写入各个local表，避免单节点数据过多
//        if (null != row) {
//            Random random = new Random();
//            int index = random.nextInt(this.ips.length);
//            switch (index) {
//
//                case 0:
//                    if(list.size() >= this.insertCkBatchSize || isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(0),connectionList.get(0));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//
//                    break;
//                case 1:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(1),connectionList.get(1));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//
//                    break;
//                case 2:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(2),connectionList.get(2));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//                    break;
//                case 3:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(3),connectionList.get(3));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//
//                    break;
//                case 4:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(4),connectionList.get(4));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//
//                    break;
//                case 5:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(5),connectionList.get(5));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//
//                    break;
//
//                case 6:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(6),connectionList.get(6));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//                    break;
//                case 7:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(7),connectionList.get(7));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//                    break;
//
//                case 8:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(8),connectionList.get(8));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//                    break;
//                case 9:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(9),connectionList.get(9));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//
//                    break;
//                case 10:
//                    if(list.size() >= this.insertCkBatchSize || this.isTimeToDoInsert()) {
//                        insertData(list,preparedStatementList.get(10),connectionList.get(10));
//                        list.clear();
//                        this.lastInsertTime = System.currentTimeMillis();
//                    } else {
//                        list.add(row);
//                    }
//                    break;
//
//            }
//        }
//    }
//
//    @Override
//    public void close() throws Exception {
//
//        for (Statement statement : this.statementList) {
//            if (null != statement) {
//                statement.close();
//            }
//        }
//
//        for (PreparedStatement preparedStatement : this.preparedStatementList) {
//            if (null != preparedStatement) {
//                preparedStatement.close();
//            }
//        }
//
//        for (Connection connection : this.connectionList) {
//            if (null != connection) {
//                connection.close();
//            }
//        }
//    }
//
//    /**
//     * 根据时间判断是否插入数据
//     *
//     * @return
//     */
//    private boolean isTimeToDoInsert() {
//        long currTime = System.currentTimeMillis();
//        return currTime - this.lastInsertTime >= this.insertCkTimenterval;
//    }
//}
