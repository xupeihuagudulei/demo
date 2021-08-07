package com.jsy.work;

import com.jsy.highfeature.BroadcastStateDemo;
import com.jsy.work.entity.UserBehavior;
import com.jsy.work.util.JacksonUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Collector;

import java.util.Map;
import java.util.Properties;

/**
 * dmp代码重构demo
 * 要求：原来broadcast join kafka，然后在join里面join dcs
 * 改为把dcs改为hive的表，使用状态存储hive表
 * <p>
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/state/state.html#using-keyed-state
 *
 * @Author: jsy
 * @Date: 2021/3/26 22:31
 * @see com.jsy.work.tool.GenHiveData
 */

/*
创建hive表


*/
public class RefactorFlink {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        //准备kafka连接参数
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "node1:9092");//集群地址
        props.setProperty("group.id", "flink");//消费者组id
        // latest有offset记录从记录位置开始消费,没有记录从最新的/最后的消息开始消费
        // earliest有offset记录从记录位置开始消费,没有记录从最早的/最开始的消息开始消费
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("flink.partition-discovery.interval-millis", "5000");//会开启一个后台线程每隔5s检测一下Kafka的分区情况,实现动态分区检测
        props.setProperty("enable.auto.commit", "true");//自动提交(提交到默认主题,后续学习了Checkpoint后随着Checkpoint存储在Checkpoint和默认主题中)
        props.setProperty("auto.commit.interval.ms", "2000");//自动提交的时间间隔
        //使用连接参数创建FlinkKafkaConsumer/kafkaSource
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<String>("user_behavior", new SimpleStringSchema(), props);
        //使用kafkaSource
        DataStream<String> kafkaDS = env.addSource(kafkaSource);

        // 定义状态描述器
        MapStateDescriptor<Void, Map<String, Tuple2<String, Integer>>> descriptor =
                new MapStateDescriptor<>("info", Types.VOID, Types.MAP(Types.STRING, Types.TUPLE(Types.STRING, Types.INT)));
        BroadcastStream<Map<String, Tuple2<String, Integer>>> broadcastStream = env.addSource(new BroadcastStateDemo.MySQLSource())
                .broadcast(descriptor);

        SingleOutputStreamOperator<UserBehavior> resultStream = kafkaDS.connect(broadcastStream).process(new BroadcastProcessFunction<String, Map<String, Tuple2<String, Integer>>, UserBehavior>() {
            @Override
            public void processElement(String value, ReadOnlyContext ctx, Collector<UserBehavior> out) throws Exception {

                ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                Map<String, Tuple2<String, Integer>> map = broadcastState.get(null);
                if (MapUtils.isEmpty(map)) {
                    return;
                }

                UserBehavior userBehavior = JacksonUtil.json2Bean(value, UserBehavior.class);

                Tuple2<String, Integer> user_1 = map.get("user_1");

                userBehavior.setBehavior(userBehavior.getBehavior() + user_1.f0);

                out.collect(userBehavior);

            }

            @Override
            public void processBroadcastElement(Map<String, Tuple2<String, Integer>> value, Context ctx, Collector<UserBehavior> out) throws Exception {

                //value就是从MySQL中每隔5是查询出来并广播到状态中的最新数据!
                //要把最新的数据放到state中
                BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                broadcastState.clear();//清空旧数据
                broadcastState.put(null, value);//放入新数据
            }
        });

        resultStream.print("broad cast end");











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

    public static class TwoStreamJoin<IN1, IN2, OUT> implements TwoInputStreamOperator<IN1, IN2, OUT> {

        @Override
        public void processElement1(StreamRecord element) throws Exception {

        }

        @Override
        public void processElement2(StreamRecord element) throws Exception {

        }

        @Override
        public void processWatermark1(Watermark mark) throws Exception {

        }

        @Override
        public void processWatermark2(Watermark mark) throws Exception {

        }

        @Override
        public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {

        }

        @Override
        public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {

        }

        @Override
        public void open() throws Exception {

        }

        @Override
        public void close() throws Exception {

        }

        @Override
        public void dispose() throws Exception {

        }

        @Override
        public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {

        }

        @Override
        public OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, CheckpointStreamFactory storageLocation) throws Exception {
            return null;
        }

        @Override
        public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {

        }

        @Override
        public MetricGroup getMetricGroup() {
            return null;
        }

        @Override
        public OperatorID getOperatorID() {
            return null;
        }

        @Override
        public void setKeyContextElement2(StreamRecord record) throws Exception {

        }

        @Override
        public void setKeyContextElement1(StreamRecord record) throws Exception {

        }

        @Override
        public void notifyCheckpointComplete(long l) throws Exception {

        }

        @Override
        public void setCurrentKey(Object key) {

        }

        @Override
        public Object getCurrentKey() {
            return null;
        }
    }

}

class UDFJoin{

}