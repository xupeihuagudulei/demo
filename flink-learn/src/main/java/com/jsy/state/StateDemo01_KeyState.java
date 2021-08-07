package com.jsy.state;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用KeyState中的ValueState获取流数据中的最大值/实际中可以使用maxBy即可
 * <p>
 * - State
 * - ManagerState     --开发中推荐使用 : Fink自动管理/优化,支持多种数据结构
 * - KeyState       --只能在keyedStream （即keyBy之后）上使用,支持多种数据结构
 * - OperatorState    --一般用在Source上,支持ListState（可以用于所有算子）
 * - RawState           --完全由用户自己管理,只支持byte[],只能在自定义Operator上使用
 * - OperatorState
 * <p>
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/state/state.html#using-keyed-state
 *
 * @Author: jsy
 * @Date: 2021/3/26 22:31
 */
public class StateDemo01_KeyState {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<Tuple2<String, Long>> tupleDS = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L)
        );

        //TODO 2.transformation
        //需求:求各个城市的value最大值
        //实际中使用maxBy即可
        DataStream<Tuple2<String, Long>> result1 = tupleDS.keyBy(t -> t.f0).maxBy(1);

        //学习时可以使用KeyState中的ValueState来实现maxBy的底层
        DataStream<Tuple3<String, Long, Long>> result2 = tupleDS.keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
                    //-1.定义一个状态用来存放最大值
                    private ValueState<Long> maxValueState;

                    //-2.状态初始化
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //创建状态描述器,名字和存的东西
                        ValueStateDescriptor stateDescriptor = new ValueStateDescriptor("maxValueState", Long.class);
                        //根据状态描述器获取/初始化状态
                        maxValueState = getRuntimeContext().getState(stateDescriptor);
                    }

                    //-3.使用状态
                    @Override
                    public Tuple3<String, Long, Long> map(Tuple2<String, Long> value) throws Exception {
                        Long currentValue = value.f1;
                        //获取状态
                        Long historyValue = maxValueState.value();
                        //判断状态
                        if (historyValue == null || currentValue > historyValue) {
                            historyValue = currentValue;
                            //更新状态
                            maxValueState.update(historyValue);
                            return Tuple3.of(value.f0, currentValue, historyValue);
                        } else {
                            return Tuple3.of(value.f0, currentValue, historyValue);
                        }
                    }
                });

        //TODO 3.sink
        result1.print("result1--->");
        //4> (北京,6)
        //1> (上海,8)
        result2.print("result3--->");
        //1> (上海,xxx,8)
        //4> (北京,xxx,6)

        //TODO 4.execute
        env.execute();
    }
}
