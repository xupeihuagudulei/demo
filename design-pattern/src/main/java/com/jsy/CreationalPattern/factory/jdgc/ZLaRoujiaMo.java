package com.jsy.CreationalPattern.factory.jdgc;

public class ZLaRoujiaMo extends RoujiaMo {

    public ZLaRoujiaMo(){
        this.name = "辣味肉夹馍";
    }

    @Override
    public void prepare() {

        System.out.println("---RoujiaMo:" + name + ": 揉面-剁肉-完成准备工作 --> 辣肉夹馍的准备");
    }
}
