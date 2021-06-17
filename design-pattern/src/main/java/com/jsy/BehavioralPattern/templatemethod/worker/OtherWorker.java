package com.jsy.BehavioralPattern.templatemethod.worker;

import com.jsy.BehavioralPattern.templatemethod.Worker;

/**
 * @Author: jsy
 * @Date: 2021/6/17 0:02
 */
public class OtherWorker extends Worker {

    public OtherWorker(String name) {
        super(name);
    }

    @Override
    public void work() {
        System.out.println("--work ---" + name + ",打LOL...");
    }
}
