package com.jsy.StructuralPattern.strategy.paydemo;

/**
 * https://mp.weixin.qq.com/s/-vP6J1NnjEy8x_IwCiTV2g
 * 通过枚举来优雅的选择支付类型，共用一个支付接口，不同的支付方式实现自己的逻辑，更加贴合面向对象的思想。
 *
 * @Author: jsy
 * @Date: 2021/5/2 21:15
 */
public class PayMain {

    public static void main(String[] args) {

        Long goodsId;
        String type = "1";

        // 生成本地的订单
        // Order order = this.orderService.makeOrder(goodsId);
        //选择支付方式
        PayType payType = PayType.getByCode(type);
        //进行支付
        payType.get().pay(1L, 10);
    }
}
