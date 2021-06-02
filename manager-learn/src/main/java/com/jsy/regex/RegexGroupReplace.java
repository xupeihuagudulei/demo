package com.jsy.regex;

/**
 * 正则分组替换，取组内值
 *
 * @Author: jsy
 * @Date: 2021/6/2 22:30
 */
public class RegexGroupReplace {

    public static void main(String[] args) {
        String str = "((appUsage_115452/appUsage_115450>0&&appUsage_115452/appUsage_115450<0.5))";
        String pattern = "([^!>=<|&()+*-/]{1,}_\\d{1,})/([^!>=<|&()+*-/]{1,}_\\d{1,})";

        str.replaceAll(pattern, "\\$didMetric.divide($1,$2)");

        System.out.println(str);

    }
}
