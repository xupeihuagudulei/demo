package com.jsy.StructuralPattern.strategy.paydemo.impl;

import com.jsy.StructuralPattern.strategy.paydemo.Payment;

/**
 * 支付宝支付
 *
 * @Author: jsy
 * @Date: 2021/5/2 21:18
 */
public class AliPay implements Payment {
    @Override
    public void pay(Long order, double amount) {
        System.out.println("----支付宝支付----");
        System.out.println("支付宝支付111元");
    }
}