package com.jsy.StructuralPattern.strategy.paydemo;

/**
 * 支付接口
 *
 * @Author: jsy
 * @Date: 2021/5/2 21:18
 */
public interface Payment {
    void pay(Long order, double amount);
}
