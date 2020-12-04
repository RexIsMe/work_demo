package com.rex.demo.study.demo.util;

import com.rex.demo.study.demo.enums.DateEnum;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalUnit;

/**
 * 日期工具类
 *
 * @Author li zhiqang
 * @create 2020/11/24
 */
public class DateUtils {
    private DateUtils() {
    }

    public static Long getMillis(DateEnum dateEnum) {
        Long mills;
        switch (dateEnum) {
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

    /**
     * @param time
     * @param pattern
     * @param offset
     * @param unit
     * @return
     */
    public static Long parseStringToLong(String time, DateTimeFormatter pattern, int offset, TemporalUnit unit) {
//        DateTimeFormatter pattern = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        LocalDateTime dateTime = null;
        if (offset > 0) {
            dateTime = LocalDateTime.parse(time, pattern).plus(offset, unit);
        } else if (offset < 0) {
            dateTime = LocalDateTime.parse(time, pattern).minus(Math.abs(offset), unit);
        } else {
            dateTime = LocalDateTime.parse(time, pattern);
        }
        return dateTime.toInstant(ZoneOffset.of("+8")).toEpochMilli();


    }


}
