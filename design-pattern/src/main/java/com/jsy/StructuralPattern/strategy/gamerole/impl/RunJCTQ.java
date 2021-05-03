package com.jsy.StructuralPattern.strategy.gamerole.impl;

import com.jsy.StructuralPattern.strategy.gamerole.IRunBehavior;

/**
 * @Author: jsy
 * @Date: 2021/5/2 21:34
 */
public class RunJCTQ implements IRunBehavior {

    @Override
    public void run() {
        System.out.println("金蝉脱壳");
    }

}