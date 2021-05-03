package com.jsy.CreationalPattern.factory.cxgc;


/**
 * 抽象工厂模式
 * 定义：提供一个接口，用于创建相关的或依赖对象的家族，而不需要明确指定具体类。
 * 这定义有点绕口，算了，还是拿例子来说。继续卖肉夹馍，咱们生意这么好，难免有些分店开始动歪脑子，
 * 开始使用劣质肉等，砸我们的品牌。所以我们要拿钱在每个城市建立自己的原料场，保证高质量原料的供应。
 *
 * <p>
 * 在北京和西安 开分店:
 * 工厂方法模式:
 * 定义：定义一个创建对象的接口，但由子类决定要实例化的类是哪一个。
 * 工厂方法模式把类实例化的过程推迟到子类。
 * <p>
 * 对照定义：
 * 1、定义了创建对象的一个接口：public abstract RouJiaMo sellRoujiaMo(String type);
 * 2、由子类决定实例化的类，可以看到我们的馍是子类生成的。
 * <p>
 * <p>
 * RoujiaMo使用的是本 抽象工厂内的类
 */

public abstract class RoujiaMoStore {

    public abstract RoujiaMo sellRoujiaMo(String type);

//    public RoujiaMo sellRoujiaMo(String type) {
//
//        RoujiaMo roujiaMo = creatRoujiaMo(type);
//        roujiaMo.prepare();
//        roujiaMo.fire();
//        roujiaMo.pack();
//        return roujiaMo;
//
//    }

}
