package com.jsy.BehavioralPattern.templatemethod.worker;

import com.jsy.BehavioralPattern.templatemethod.Worker;

/**
 * @Author: jsy
 * @Date: 2021/6/17 0:00
 */
public class HRWorker extends Worker {

    public HRWorker(String name) {
        super(name);
    }

    @Override
    public void work() {
        System.out.println("--work ---" + name + ", 看简历 - 打电话 - 接电话");
    }
}
