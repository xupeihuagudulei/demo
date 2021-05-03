package com.jsy.StructuralPattern.strategy.gamerole;

import com.jsy.StructuralPattern.strategy.gamerole.impl.AttackXL;
import com.jsy.StructuralPattern.strategy.gamerole.impl.DefendTBS;
import com.jsy.StructuralPattern.strategy.gamerole.impl.DisplayYZ;
import com.jsy.StructuralPattern.strategy.gamerole.impl.RunJCTQ;

/**
 * 策略模式（Strategy Pattern）：定义了算法族，分别封装起来，
 * 让它们之间可相互替换，此模式让算法的变化独立于使用算法的客户。
 * <p>
 * https://blog.csdn.net/lmj623565791/article/details/24116745
 * 定义上的算法族：其实就是上述例子的技能；
 * 定义上的客户：其实就是RoleA，RoleB...；
 * 我们已经定义了一个算法族（各种技能），且根据需求可以进行相互替换，算法（各种技能）的实现独立于客户（角色）。
 * <p>
 * 需求：
 * 假设公司需要做一款武侠游戏，我们就是负责游戏的角色模块，需求是这样的：每个角色对应一个名字，每类角色对应一种样子，、
 * 每个角色拥有一个逃跑、攻击、防御的技能。
 *
 * @Author: jsy
 * @Date: 2021/5/2 21:38
 */
public class GameRoleTest {
    public static void main(String[] args) {

        // 每个角色现在只需要一个name了
        Role roleA = new RoleA("A");

        roleA.setAttackBehavior(new AttackXL())//
                .setDefendBehavior(new DefendTBS())//
                .setDisplayBehavior(new DisplayYZ())//
                .setRunBehavior(new RunJCTQ());
        System.out.println(roleA.name + ":");
        roleA.run();
        roleA.attack();
        roleA.defend();
        roleA.display();
    }

}

/**
 * 策略模式与状态模式
 *
 * 可以通过环境类状态的个数来决定是使用策略模式还是状态模式。
 * 策略模式的环境类自己选择一个具体策略类，具体策略类无须关心环境类；而状态模式的环境类由于外在因素需要放进一个具体状态中，以便通过其方法实现状态的切换，因此环境类和状态类之间存在一种双向的关联关系。
 * 使用策略模式时，客户端需要知道所选的具体策略是哪一个，而使用状态模式时，客户端无须关心具体状态，环境类的状态会根据用户的操作自动转换。
 * 如果系统中某个类的对象存在多种状态，不同状态下行为有差异，而且这些状态之间可以发生转换时使用状态模式；如果系统中某个类的某一行为存在多种实现方式，而且这些实现方式可以互换时使用策略模式。
 */