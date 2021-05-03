package com.jsy.StructuralPattern.strategy.gamerole.impl;

import com.jsy.StructuralPattern.strategy.gamerole.IAttackBehavior;

/**
 * @Author: jsy
 * @Date: 2021/5/2 21:33
 */
public class AttackJY implements IAttackBehavior {

    @Override
    public void attack() {
        System.out.println("九阳神功！");
    }

}
