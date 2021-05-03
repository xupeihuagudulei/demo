package com.jsy.CreationalPattern.builder;

/**
 * 产品类
 *
 * @Author: jsy
 * @Date: 2021/5/2 22:26
 */
public class Product {

    private String name;
    private String type;

    public void showProduct() {
        System.out.println("名称：" + name);
        System.out.println("型号：" + type);
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setType(String type) {
        this.type = type;
    }
}