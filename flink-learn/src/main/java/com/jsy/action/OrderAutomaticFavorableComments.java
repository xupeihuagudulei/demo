package com.jsy.action;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

/**
 * 定时器实现自动好评
 * Flink CEP也可以做，java web也可以做
 * <p>
 * 在电商领域会有这么一个场景，如果用户买了商品，在订单完成之后，一定时间之内没有做出评价，系统自动给与五星好评，我们今天主要使用Flink的定时器来简单实现这一功能。
 *
 * @Author: jsy
 * @Date: 2021/4/6 23:58
 */
public class OrderAutomaticFavorableComments {

    public static void main(String[] args) throws Exception {
        //TODO 1.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        env.setParallelism(1);// 方便观察

        //TODO 2.source
        //Tuple3<用户id,订单id,订单生成时间>
        DataStream<Tuple3<String, String, Long>> orderDS = env.addSource(new MySource());

        //TODO 3.transformation
        //设置经过interval毫秒用户未对订单做出评价，自动给与好评.为了演示方便，设置5s的时间
        long interval = 5000L;//5s  实际是多少天
        //分组后使用自定义KeyedProcessFunction完成定时判断超时订单并自动好评
        // 窗口后的process叫 ProcessWindowFunction ，没有window是ProcessFunction
        orderDS.keyBy(t -> t.f0)
                .process(new TimerProcessFunction(interval));

        //TODO 4.sink

        //TODO 5.execute
        env.execute();
    }

    /**
     * 自定义source实时产生订单数据Tuple3<用户id,订单id, 订单生成时间>
     */
    public static class MySource implements SourceFunction<Tuple3<String, String, Long>> {
        private boolean flag = true;

        @Override
        public void run(SourceContext<Tuple3<String, String, Long>> ctx) throws Exception {
            Random random = new Random();
            while (flag) {
                String userId = random.nextInt(5) + "";
                String orderId = UUID.randomUUID().toString();
                long currentTimeMillis = System.currentTimeMillis();
                ctx.collect(Tuple3.of(userId, orderId, currentTimeMillis));
                Thread.sleep(500);
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }
    }

    /**
     * 自定义ProcessFunction完成订单自动好评
     * 进来一条数据应该在interval时间后进行判断该订单是否超时是否需要自动好评
     * abstract class KeyedProcessFunction<K, I, O>
     */
    private static class TimerProcessFunction extends KeyedProcessFunction<String, Tuple3<String, String, Long>, Object> {
        private long interval;//订单超时时间 传进来的是5000ms/5s

        public TimerProcessFunction(long interval) {
            this.interval = interval;
        }

        //-0.准备一个State来存储订单id和订单生成时间
        private MapState<String, Long> mapState = null;

        //-1.初始化
        @Override
        public void open(Configuration parameters) throws Exception {
            MapStateDescriptor<String, Long> mapStateDescriptor = new MapStateDescriptor<>("mapState", String.class, Long.class);
            mapState = getRuntimeContext().getMapState(mapStateDescriptor);
        }

        //-2.处理每一条数据并存入状态并注册定时器
        @Override
        public void processElement(Tuple3<String, String, Long> value, Context ctx, Collector<Object> out) throws Exception {
            //Tuple3<用户id,订单id, 订单生成时间> value里面是当前进来的数据里面有订单生成时间
            //把订单数据保存到状态中
            mapState.put(value.f1, value.f2);//xxx,2020-11-11 00:00:00 || xx,2020-11-11 00:00:01
            //该订单在value.f2 + interval时过期/到期,这时如果没有评价的话需要系统给与默认好评
            // TODO 注册一个定时器在 value.f2 + interval时检查是否需要默认好评
            ctx.timerService().registerProcessingTimeTimer(value.f2 + interval);//2020-11-11 00:00:05  || 2020-11-11 00:00:06
        }

        //-3.执行定时任务
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Object> out) throws Exception {
            //检查历史订单数据(在状态中存储着)
            //遍历取出状态中的订单数据
            Iterator<Map.Entry<String, Long>> iterator = mapState.iterator();
            while (iterator.hasNext()) {
                Map.Entry<String, Long> map = iterator.next();
                String orderId = map.getKey();
                Long orderTime = map.getValue();
                //先判断是否好评--实际中应该去调用订单评价系统看是否好评了,我们这里写个方法模拟一下
                if (!isFavorable(orderId)) {//该订单没有给好评
                    //判断是否超时--不用考虑进来的数据是否过期,统一判断是否超时更保险!
                    if (System.currentTimeMillis() - orderTime >= interval) {
                        System.out.println("orderId:" + orderId + "该订单已经超时未评价,系统自动给与好评!....");
                        //移除状态中的数据,避免后续重复判断
                        iterator.remove();
                        mapState.remove(orderId);
                    }
                } else {
                    System.out.println("orderId:" + orderId + "该订单已经评价....");
                    //移除状态中的数据,避免后续重复判断
                    iterator.remove();
                    mapState.remove(orderId);
                }
            }
        }

        //自定义一个方法模拟订单系统返回该订单是否已经好评,实际中是查数据库的
        public boolean isFavorable(String orderId) {
            return orderId.hashCode() % 2 == 0;
        }
    }
}
