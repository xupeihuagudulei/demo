package com.jsy.StructuralPattern.strategy.paydemo.impl;

import com.jsy.StructuralPattern.strategy.paydemo.Payment;

/**
 * 微信支付
 *
 * @Author: jsy
 * @Date: 2021/5/2 21:18
 */
public class WechatPay implements Payment {
    @Override
    public void pay(Long orderId, double amount) {
        System.out.println("---微信支付---");
        System.out.println("支付222元");
    }
}