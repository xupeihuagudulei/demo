package com.jsy.CreationalPattern.builder;

/**
 * 导演类
 *
 * @Author: jsy
 * @Date: 2021/5/2 22:25
 */
public class Director {

    private Builder builder = new ConcreteBuilder();

    public Product getAProduct() {
        builder.setPart("奥迪汽车", "Q5");
        return builder.getProduct();
    }

    public Product getBProduct() {
        builder.setPart("宝马汽车", "X7");
        return builder.getProduct();
    }
}
