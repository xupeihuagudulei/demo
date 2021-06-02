package com.jsy.aaa.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.AggregateFunction;
import org.apache.flink.types.Row;

/**
 * @Author: jsy
 * @Date: 2021/5/26 0:17
 */
class MyMinMaxAcc {
    public int min = 0;
    public int max = 0;
}

public class MyMinMax extends AggregateFunction<Row, MyMinMaxAcc> {

    public void accumulate(MyMinMaxAcc acc, int value) {
        if (value < acc.min) {
            acc.min = value;
        }
        if (value > acc.max) {
            acc.max = value;
        }
    }

    @Override
    public MyMinMaxAcc createAccumulator() {
        return new MyMinMaxAcc();
    }

    public void resetAccumulator(MyMinMaxAcc acc) {
        acc.min = 0;
        acc.max = 0;
    }

    @Override
    public Row getValue(MyMinMaxAcc acc) {
        return Row.of(acc.min, acc.max);
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(Types.INT, Types.INT);
    }
}