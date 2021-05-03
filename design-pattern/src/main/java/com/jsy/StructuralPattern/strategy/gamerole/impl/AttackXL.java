package com.jsy.StructuralPattern.strategy.gamerole.impl;

import com.jsy.StructuralPattern.strategy.gamerole.IAttackBehavior;

/**
 * @Author: jsy
 * @Date: 2021/5/2 21:39
 */
public class AttackXL implements IAttackBehavior {
    @Override
    public void attack() {
        System.out.println("降龙十八掌");
    }
}
