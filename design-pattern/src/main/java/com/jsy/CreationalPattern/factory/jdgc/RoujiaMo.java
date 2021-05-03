package com.jsy.CreationalPattern.factory.jdgc;

/**
 * 各种风味的馍馍
 */

public abstract class RoujiaMo {

    protected String name;

    /**
     * 准备工作
     */
    public void prepare() {
        System.out.println("---RoujiaMo:" + name + ": 揉面-剁肉-完成准备工作");
    }

    /**
     * 秘制设备--烘烤2分钟
     */
    public void fire() {
        System.out.println("---RoujiaMo:" + name + ": 肉夹馍-专用设备-烘烤");
    }

    /**
     * 使用你们的专用袋-包装
     */
    public void pack() {
        System.out.println("---RoujiaMo:" + name + ": 肉夹馍-专用袋-包装---end");
    }
}
