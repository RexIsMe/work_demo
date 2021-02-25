package com.rex.demo.study.demo.entity;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;

import java.util.List;

/**
 * thalys dc 数据对象
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */
@Data
public class DCRecordEntity {

    private String ip;
    private Long timestamp;
    private String companyName;
    private String businessName;
    private List<JSONObject> dataList;

}