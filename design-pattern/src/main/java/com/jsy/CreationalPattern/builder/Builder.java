package com.jsy.CreationalPattern.builder;

/**
 * 抽象建造者
 *
 * @Author: jsy
 * @Date: 2021/5/2 22:22
 */
public abstract class Builder {

    public abstract void setPart(String name, String type);

    public abstract Product getProduct();
}