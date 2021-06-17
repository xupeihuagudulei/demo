package com.jsy.BehavioralPattern.templatemethod.worker;

import com.jsy.BehavioralPattern.templatemethod.Worker;

/**
 * @Author: jsy
 * @Date: 2021/6/16 23:59
 */
public class CTOWorker extends Worker {

    public CTOWorker(String name) {
        super(name);
    }

    @Override
    public void work() {
        System.out.println("--work ---" + name + ", 开会 - 出API - 检查进度");
    }
}