package com.jsy.window;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 基于时间的滚动和滑动窗口
 *
 * 使用keyBy的流,应该使用window方法
 * 未使用keyBy的流,应该调用windowAll方法
 *
 * <p>
 * 需求
 * nc -lk 9999
 * 有如下数据表示:
 * 信号灯编号和通过该信号灯的车的数量
 * 9,3
 * 9,2
 * 9,7
 * 4,9
 * 2,6
 * 5,4
 * 需求1:每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滚动窗口
 * 需求2:每5秒钟统计一次，最近10秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滑动窗口
 *
 * @Author: jsy
 * @Date: 2021/3/26 22:03
 */
public class WindowDemo1_TimeBase {

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<String> lines = env.socketTextStream("node1", 9999);
        //TODO 2.transformation
        SingleOutputStreamOperator<CartInfo> carDS = lines.map(new MapFunction<String, CartInfo>() {
            @Override
            public CartInfo map(String value) throws Exception {
                String[] arr = value.split(",");
                return new CartInfo(arr[0], Integer.parseInt(arr[1]));
            }
        });

        //注意: 需求中要求的是各个路口/红绿灯的结果,所以需要先分组
        //carDS.keyBy(car->car.getSensorId())
        KeyedStream<CartInfo, String> keyedDS = carDS.keyBy(CartInfo::getSensorId);
        // * 需求1:每5秒钟统计一次，最近5秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滚动窗口
        //keyedDS.timeWindow(Time.seconds(5))
        SingleOutputStreamOperator<CartInfo> result1 = keyedDS
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum("count");
        // * 需求2:每5秒钟统计一次，最近10秒钟内，各个路口通过红绿灯汽车的数量--基于时间的滑动窗口
        SingleOutputStreamOperator<CartInfo> result2 = keyedDS
                //of(Time size, Time slide)
                .window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .sum("count");

        //TODO 3.sink
        // 触发条件，要么时间到了，要么来数据了，不会有0
        result1.print("tumbling");
        result2.print("sliding");
/*
1,5
2,5
3,5
4,5
*/

        //TODO 4.execute
        env.execute();
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class CartInfo {
        private String sensorId;//信号灯id
        private Integer count;//通过该信号灯的车的数量
    }
}
