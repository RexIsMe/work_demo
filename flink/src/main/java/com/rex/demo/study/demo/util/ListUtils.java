package com.rex.demo.study.demo.util;

import java.util.ArrayList;

/**
 * list集合相关方法
 *
 * @Author li zhiqang
 * @create 2021/2/19
 */
public class ListUtils {

    /**
     * list的toString()可以打印具体的元素信息
     * @param args
     */
    public static void main(String[] args){

        // 创建一个动态数组
        ArrayList<String> sites = new ArrayList<>();

        sites.add("Runoob");
        sites.add("Google");
        sites.add("Wiki");
        sites.add("Taobao");
        System.out.println("网站列表: " + sites);

        // 将ArrayList转换为String类型
        String list = sites.toString();
        System.out.println("String: " + list);
    }
}
