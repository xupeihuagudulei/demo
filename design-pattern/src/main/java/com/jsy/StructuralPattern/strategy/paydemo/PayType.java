package com.jsy.StructuralPattern.strategy.paydemo;

import com.jsy.StructuralPattern.strategy.paydemo.impl.AliPay;
import com.jsy.StructuralPattern.strategy.paydemo.impl.WechatPay;

/**
 * 支付方式
 *
 * @Author: jsy
 * @Date: 2021/5/2 21:17
 */
public enum PayType {
    //支付宝        AliPay 是实现类
    ALI_PAY("1", new AliPay()),
    //微信
    WECHAT_PAY("2", new WechatPay());

    private String payType;
    // 这是一个接口
    private Payment payment;

    PayType(String payType, Payment payment) {
        this.payment = payment;
        this.payType = payType;
    }

    //通过get方法获取支付方式
    public Payment get() {
        return this.payment;
    }

    public static PayType getByCode(String payType) {
        for (PayType e : PayType.values()) {
            if (e.payType.equals(payType)) {
                return e;
            }
        }
        return null;
    }
}