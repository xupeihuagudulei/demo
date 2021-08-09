package com.jsy.work.refactor;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

/**
 * @Author: jsy
 * @Date: 2021/8/8 3:12
 */
public class CoProcess extends CoProcessFunction<BroadCastStreamOut, Map<String, HiveEntity>, String> {

    private static final String HIVE_STATE_NAME = "hive";

    private transient MapState<String, HiveEntity> hiveState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        //根据状态描述器获取/初始化状态
        hiveState = getRuntimeContext()
                .getMapState(
                        new MapStateDescriptor<>(HIVE_STATE_NAME, Types.STRING, TypeInformation.of(HiveEntity.class)));

    }

    // config stream 使用状态
    @Override
    public void processElement1(BroadCastStreamOut value, Context ctx, Collector<String> out) throws Exception {

        System.out.println("value = " + value);

        String userId = value.getUserBehavior().getUserId();

        HiveEntity hiveEntity = hiveState.get(userId);
        System.out.println("hiveEntity = " + hiveEntity);

    }

    // hive 更新状态
    @Override
    public void processElement2(Map<String, HiveEntity> value, Context ctx, Collector<String> out) throws Exception {

        //更新状态

        hiveState.putAll(value);

    }
}
