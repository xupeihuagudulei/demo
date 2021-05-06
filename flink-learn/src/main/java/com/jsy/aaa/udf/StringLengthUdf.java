package com.jsy.aaa.udf;

import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;

/**
 * UDF需要在ScalarFunction类中实现eval方法。open方法和close方法可选。
 * <p>
 * 注意 UDF默认对于相同的输入会有相同的输出。如果UDF不能保证相同的输出，例如，在UDF中调用外部服务，
 * 相同的输入值可能返回不同的结果，建议您使用override isDeterministic()方法，返回False。
 * 否则在某些条件下，输出结果不符合预期。例如，UDF算子前移。
 *
 * @Author: jsy
 * @Date: 2021/5/6 23:30
 */
public class StringLengthUdf extends ScalarFunction {
    // 可选，open方法可以不写。
    // 如果编写open方法需要声明'import org.apache.flink.table.functions.FunctionContext;'。
    @Override
    public void open(FunctionContext context) {
    }

    public long eval(String a) {
        return a == null ? 0 : a.length();
    }

    public long eval(String b, String c) {
        return eval(b) + eval(c);
    }

    //可选，close方法可以不写。
    @Override
    public void close() {
    }
}