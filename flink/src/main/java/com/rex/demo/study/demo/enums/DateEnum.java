package com.rex.demo.study.demo.enums;

/**
 * @Author li zhiqang
 * @create 2020/11/24
 */
public enum  DateEnum {

    SECOND(1),
    MINUTE(2),
    HOUR(3),
    DAY(4);

    private Integer dateFlag;

    DateEnum(Integer dateFlag) {
        this.dateFlag = dateFlag;
    }

    public Integer getDateFlag() {
        return dateFlag;
    }
}
