package com.jsy.CreationalPattern.builder;

/**
 * 具体建造者
 *
 * @Author: jsy
 * @Date: 2021/5/2 22:25
 */
public class ConcreteBuilder extends Builder {

    private Product product = new Product();

    @Override
    public void setPart(String name, String type) {
        product.setName(name);
        product.setType(type);
    }

    @Override
    public Product getProduct() {
        return product;
    }
}