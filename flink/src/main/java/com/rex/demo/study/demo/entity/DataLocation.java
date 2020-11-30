package com.rex.demo.study.demo.entity;

/**
 * TODO 实体类
 *
 * @author liuzebiao
 * @Date 2020-2-14 13:58
 */
public class DataLocation {

    public String id;

    public String name;

    public String date;

    private String longitude;

    private String latitude;

    public String province;

    public DataLocation() {
    }

    public DataLocation(String id, String name, String date, String longitude, String latitude) {
        this.id = id;
        this.name = name;
        this.date = date;
        this.longitude = longitude;
        this.latitude = latitude;
    }

    public DataLocation(String id, String name, String date, String province) {
        this.id = id;
        this.name = name;
        this.date = date;
        this.province = province;
    }

    public static DataLocation of(String id, String name, String date, String province) {
        return new DataLocation(id, name, date, province);
    }

    @Override
    public String toString() {
        return "DataLocation{" +
                "id='" + id + '\'' +
                ", name='" + name + '\'' +
                ", date='" + date + '\'' +
                ", province='" + province + '\'' +
                '}';
    }
}

