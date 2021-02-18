package com.rex.demo.study.demo.sink;


import com.alibaba.fastjson.JSONObject;
import com.rex.demo.study.demo.entity.DCRecordEntity;
import com.rex.demo.study.demo.entity.StockEntity;
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
        String sql = "insert into test.stock_data(ip ," +
                "time_stamp ," +
                "business_name ," +
                "company_name ," +
                "batch_no ," +
                "inventory_number ," +
                "warehouse_name ," +
                "warehouse_id ," +
                "warehouse_code ," +
                "warehouse_category_name ," +
                "warehouse_category_id ," +
                "warehouse_category_code ," +
                "warehouse_area_id ," +
                "warehouse_area_desc ," +
                "warehouse_area_code ," +
                "supplier_name ," +
                "supplier_id ," +
                "supplier_enable_code ," +
                "supplier_code ," +
                "storage_condition ," +
                "sterilization_batch_no ," +
                "register_number ," +
                "register_name ," +
                "quotiety ," +
                "quantity ," +
                "professional_group ," +
                "production_place ," +
                "production_license ," +
                "produced_date ," +
                "package_unit_name ," +
                "accounting_ml ," +
                "accounting_name ," +
                "accounting_one ," +
                "allocated_quantity ," +
                "brand_name ," +
                "classify_id_level1 ," +
                "classify_id_level2 ," +
                "cold_chain_mark ," +
                "commodity_code ," +
                "commodity_id ," +
                "commodity_name ," +
                "commodity_number ," +
                "commodity_remark ," +
                "commodity_spec ," +
                "commodity_type ," +
                "customer_code ," +
                "customer_enable_code ," +
                "customer_id ," +
                "customer_name ," +
                "device_classify ," +
                "device_classify_type ," +
                "effective_date ," +
                "effective_days ," +
                "goods_location ," +
                "goods_location_id ," +
                "inventory_organization_code ," +
                "inventory_organization_id ," +
                "inventory_organization_name ," +
                "is_simplelevy ," +
                "logic_area_code ," +
                "logic_area_id ," +
                "logic_area_name ," +
                "manufacturer_name ," +
                "organization_id ," +
                "organization_name ," +
                "owner_code ," +
                "owner_id ," +
                "owner_name ," +
                "package_unitId ) values(?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?,?," +
                "?,?,?,?,?,?,?,?,?);";
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
        for (String str : value) {
            DCRecordEntity dcRecordEntity = JSONObject.parseObject(str, DCRecordEntity.class);
            ps.setString(1, dcRecordEntity.getIp());
            ps.setLong(2, dcRecordEntity.getTimestamp());
            ps.setString(3, dcRecordEntity.getBusinessName());
            ps.setString(4, dcRecordEntity.getCompanyName());

            List<JSONObject> dataList = dcRecordEntity.getDataList();
            for (JSONObject jsonObject : dataList) {
                StockEntity stockEntity = jsonObject.toJavaObject(StockEntity.class);
                ps.setString(5, stockEntity.getBatchNo());
                ps.setInt(6, stockEntity.getInventoryNumber());
                ps.setString(7, stockEntity.getWarehouseName());
                ps.setInt(8, stockEntity.getWarehouseId());
                ps.setString(9, stockEntity.getWarehouseCode());
                ps.setString(10, stockEntity.getWarehouseCategoryName());
                ps.setInt(11, stockEntity.getWarehouseCategoryId());
                ps.setString(12, stockEntity.getWarehouseCategoryCode());
                ps.setInt(13, stockEntity.getWarehouseAreaId());
                ps.setString(14, stockEntity.getWarehouseAreaDesc());
                ps.setString(15, stockEntity.getWarehouseAreaCode());
                ps.setString(16, stockEntity.getSupplierName());
                ps.setInt(17, stockEntity.getSupplierId());
                ps.setString(18, stockEntity.getSupplierEnableCode());
                ps.setString(19, stockEntity.getSupplierCode());
                ps.setString(20, stockEntity.getStorageCondition());
                ps.setString(21, stockEntity.getSterilizationBatchNo());
                ps.setString(22, stockEntity.getRegisterNumber());
                ps.setString(23, stockEntity.getRegisterName());
                ps.setInt(24, stockEntity.getQuotiety());
                ps.setInt(25, stockEntity.getQuantity());
                ps.setString(26, stockEntity.getProfessionalGroup());
                ps.setString(27, stockEntity.getProductionPlace());
                ps.setString(28, stockEntity.getProductionLicense());
                ps.setLong(29, stockEntity.getProducedDate());
                ps.setString(30, stockEntity.getPackageUnitName());
                ps.setString(31, stockEntity.getAccountingMl());
                ps.setString(32, stockEntity.getAccountingName());
                ps.setString(33, stockEntity.getAccountingOne());
                ps.setInt(34, stockEntity.getAllocatedQuantity());
                ps.setString(35, stockEntity.getBrandName());
                ps.setString(36, stockEntity.getClassifyIdLevel1());
                ps.setString(37, stockEntity.getClassifyIdLevel2());
                ps.setString(38, stockEntity.getColdChainMark());
                ps.setString(39, stockEntity.getCommodityCode());
                ps.setInt(40, stockEntity.getCommodityId());
                ps.setString(41, stockEntity.getCommodityName());
                ps.setString(42, stockEntity.getCommodityNumber());
                ps.setString(43, stockEntity.getCommodityRemark());
                ps.setString(44, stockEntity.getCommoditySpec());
                ps.setString(45, stockEntity.getCommodityType());
                ps.setString(46, stockEntity.getCustomerCode());
                ps.setString(47, stockEntity.getCustomerEnableCode());
                ps.setInt(48, stockEntity.getCustomerId() == null ? 0 : stockEntity.getCustomerId());
                ps.setString(49, stockEntity.getCustomerName());
                ps.setString(50, stockEntity.getDeviceClassify());
                ps.setString(51, stockEntity.getDeviceClassifyType());
                ps.setLong(52, stockEntity.getEffectiveDate());
                ps.setInt(53, stockEntity.getEffectiveDays());
                ps.setString(54, stockEntity.getGoodsLocation());
                ps.setInt(55, stockEntity.getGoodsLocationId());
                ps.setString(56, stockEntity.getInventoryOrganizationCode());
                ps.setString(57, stockEntity.getInventoryOrganizationId());
                ps.setString(58, stockEntity.getInventoryOrganizationName());
                ps.setString(59, stockEntity.getIsSimplelevy());
                ps.setString(60, stockEntity.getLogicAreaCode());
                ps.setInt(61, stockEntity.getLogicAreaId());
                ps.setString(62, stockEntity.getLogicAreaName());
                ps.setString(63, stockEntity.getManufacturerName());
                ps.setLong(64, stockEntity.getOrganizationId());
                ps.setString(65, stockEntity.getOrganizationName());
                ps.setString(66, stockEntity.getOwnerCode());
                ps.setInt(67, stockEntity.getOwnerId());
                ps.setString(68, stockEntity.getOwnerName());
                ps.setInt(69, stockEntity.getPackageUnitId());

                ps.addBatch();
            }
        }
        int[] count = ps.executeBatch();  //批量后执行
        connection.commit();
        System.out.println("成功了插入了" + count.length + "行数据");
    }


    private static Connection getConnection(BasicDataSource dataSource) {
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        //注意，替换成自己本地的 mysql 数据库地址和用户名、密码
        dataSource.setUrl("jdbc:mysql://172.16.0.213:3306/test?autoReconnect=true&autoReconnectForPools=true&useUnicode=true&characterEncoding=utf8&useSSL=false&useAffectedRows=true&allowMultiQueries=true");
        dataSource.setUsername("root");
        dataSource.setPassword("123456");
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

