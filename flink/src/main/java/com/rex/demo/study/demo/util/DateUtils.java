package com.rex.demo.study.demo.util;

import com.rex.demo.study.demo.enums.DateEnum;

/**
 * 日期工具类
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */
public class DateUtils {
    private DateUtils() {
    }

    public static Long getMillis(DateEnum dateEnum){
        Long mills;
        switch (dateEnum){
            case SECOND:
                mills = 1 * 1000L;
                break;
            case MINUTE:
                mills = 60 * 1000L;
                break;
            case HOUR:
                mills = 60 * 60 * 1000L;
                break;
            case DAY:
                mills = 24 * 60 * 60 * 1000L;
                break;
            default:
                mills = 1L;
                break;
        }
        return mills;
    }

}
