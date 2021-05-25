package com.jsy.other;

import com.google.common.base.Splitter;

import java.util.List;

/**
 * @Author: jsy
 * @Date: 2021/5/24 12:38
 */
public class OtherTest {
    public static void main(String[] args) {

        List<String> strings = Splitter.on(";").trimResults().omitEmptyStrings().splitToList("aa;ab");

        for (String string : strings) {
            System.out.println("string = " + string);
        }
    }
}
