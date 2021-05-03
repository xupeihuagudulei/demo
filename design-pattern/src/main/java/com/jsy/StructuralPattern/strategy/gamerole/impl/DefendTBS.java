package com.jsy.StructuralPattern.strategy.gamerole.impl;

import com.jsy.StructuralPattern.strategy.gamerole.IDefendBehavior;

/**
 * @Author: jsy
 * @Date: 2021/5/2 21:33
 */

public class DefendTBS implements IDefendBehavior {

    @Override
    public void defend() {
        System.out.println("铁布衫");
    }

}