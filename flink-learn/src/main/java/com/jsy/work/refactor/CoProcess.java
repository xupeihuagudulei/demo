package com.jsy.work.refactor;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.util.Collector;

/**
 * @Author: jsy
 * @Date: 2021/8/8 3:12
 */
public class CoProcess extends CoProcessFunction {

    private static final String HIVE_STATE_NAME = "hive";

    private transient MapState<String, RowData> hiveState;

    private final InternalTypeInfo<RowData> leftType;

    public CoProcess(InternalTypeInfo<RowData> leftType) {
        this.leftType = leftType;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //根据状态描述器获取/初始化状态
        hiveState = getRuntimeContext()
                .getMapState(
                        new MapStateDescriptor<>(HIVE_STATE_NAME, Types.STRING, leftType));

    }

    // config stream 使用状态
    @Override
    public void processElement1(Object value, Context ctx, Collector out) throws Exception {


    }

    // hive 更新状态
    @Override
    public void processElement2(Object value, Context ctx, Collector out) throws Exception {

        //获取状态
        // Long historyValue = hiveState.entries()


        //判断状态
        // if (historyValue == null || currentValue > historyValue) {
        //     historyValue = currentValue;
        //     //更新状态
        //     maxValueState.update(historyValue);
        //     return Tuple3.of(value.f0, currentValue, historyValue);
        // } else {
        //     return Tuple3.of(value.f0, currentValue, historyValue);
        // }


    }


}
