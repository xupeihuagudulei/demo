package com.jsy.CreationalPattern.factory.gcff;

import com.jsy.CreationalPattern.factory.jdgc.RoujiaMo;

/**
 * 在北京和西安 开分店:
 *
 * 工厂方法模式:
 * 定义：定义一个创建对象的接口，但由子类决定要实例化的类是哪一个。
 * 工厂方法模式把类实例化的过程推迟到子类。
 *
 * 对照定义：
 1、定义了创建对象的一个接口：public abstract RouJiaMo sellRoujiaMo(String type);
 2、由子类决定实例化的类，可以看到我们的馍是子类生成的。
 */

public abstract class RoujiaMoStore {

    public abstract RoujiaMo sellRoujiaMo(String type);

   //  /**
   //   * 根据传入类型卖不同的肉夹馍
   //   *
   //   * @param type
   //   * @return
   //   */
   // public RoujiaMo sellRoujiaMo(String type) {
   //
   //     RoujiaMo roujiaMo = creatRoujiaMo(type);
   //     roujiaMo.prepare();
   //     roujiaMo.fire();
   //     roujiaMo.pack();
   //     return roujiaMo;
   //
   // }

}
