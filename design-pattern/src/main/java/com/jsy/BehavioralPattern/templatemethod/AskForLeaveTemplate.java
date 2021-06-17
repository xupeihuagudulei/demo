package com.jsy.BehavioralPattern.templatemethod;

/**
 * 模板方法 demo2
 *
 * 模版方法模式的定义以及目的？
 * <p>
 * 定义：模板方法模式在一个方法中定义一个算法骨架，并将某些步骤推迟到子类中实现。模板方法模式可以让子类在不改变算法整体结构的情况下，重新定义算法中的某些步骤
 * <p>
 * 目的：1.使用模版方法模式的目的是避免编写重复代码，以便开发人员可以专注于核心业务逻辑的实现
 * <p>
 * 2.解决接口与接口实现类之间继承矛盾问题
 * <p>
 * 以上定义来自《设计模式之美》
 * <p>
 * https://mp.weixin.qq.com/s/oGm4ET-NDp4BSVi0p2MZxA
 *
 * @Author: jsy
 * @Date: 2021/6/16 23:49
 */
public class AskForLeaveTemplate {
    public static void main(String[] args) {
        // 公司A请假流程模版
        AskForLeaveFlow companyA = new CompanyA();
        companyA.askForLeave("敖丙");
        // 结果：CompanyA 组内有人请假，请假人：敖丙
        //       当前有人请假了，请假人：敖丙

        AskForLeaveFlow companyB = new CompanyB();
        companyB.askForLeave("敖丙");
        // 结果：CompanyB 组内有人请假，请假人：敖丙
        //      CompanyB 部门有人请假，请假人：敖丙
        //      当前有人请假了，请假人：敖丙
    }

}

abstract class AskForLeaveFlow {

    // 一级组长直接审批
    protected abstract void firstGroupLeader(String name);

    // 二级组长部门负责人审批
    protected void secondGroupLeader(String name) {
    }

    // 告知HR有人请假了
    private final void notifyHr(String name) {
        System.out.println("当前有人请假了，请假人：" + name);
    }

    // 请假流模版
    public void askForLeave(String name) {
        firstGroupLeader(name);
        secondGroupLeader(name);
        notifyHr(name);
    }

}

class CompanyA extends AskForLeaveFlow {

    @Override
    protected void firstGroupLeader(String name) {
        System.out.println("CompanyA 组内有人请假，请假人：" + name);
    }
}

class CompanyB extends AskForLeaveFlow {
    @Override
    protected void firstGroupLeader(String name) {
        System.out.println("CompanyB 组内有人请假，请假人：" + name);
    }

    @Override
    protected void secondGroupLeader(String name) {
        System.out.println("CompanyB 部门有人请假，请假人：" + name);
    }
}