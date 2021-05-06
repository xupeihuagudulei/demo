package com.jsy.aaa.udf;

import org.apache.flink.table.functions.ScalarFunction;

/**
 * @Author: jsy
 * @Date: 2021/5/7 0:23
 */
public class IsStatus extends ScalarFunction {
    private int status = 0;

    public IsStatus(int status) {
        this.status = status;
    }

    public boolean eval(int status) {
        if (this.status == status) {
            return true;
        } else {
            return false;
        }
    }
}
