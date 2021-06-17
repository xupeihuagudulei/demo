package com.jsy.BehavioralPattern.templatemethod.worker;

import com.jsy.BehavioralPattern.templatemethod.Worker;

/**
 * @Author: jsy
 * @Date: 2021/6/17 0:01
 */
public class ITWorker extends Worker {

    public ITWorker(String name) {
        super(name);
    }

    /**
     * 重写父类的此方法,使可以查看离开公司时间
     */
    @Override
    public boolean isNeedPrintDate() {
        return true;
    }

    @Override
    public void work() {
        System.out.println("--work ---" + name + ", 写程序 - 测bug - 修复bug");
    }
}
