package com.jsy.work.refactor;

import com.jsy.work.util.JacksonUtil;
import org.apache.commons.collections.MapUtils;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * dmp代码重构demo
 * 要求：原来broadcast join kafka，然后在join里面join dcs
 * 改为把dcs改为hive的表，使用状态存储hive表
 * <p>
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/stream/state/state.html#using-keyed-state
 *
 * https://blog.csdn.net/andyonlines/article/details/108173259
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
        BroadcastStream<Map<String, Tuple2<String, Integer>>> broadcastStream = env.addSource(new MySQLSource())
                .broadcast(descriptor);

        DataStream<BroadCastStreamOut> configStream = kafkaDS.connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, Map<String, Tuple2<String, Integer>>, BroadCastStreamOut>() {
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<BroadCastStreamOut> out) throws Exception {

                        ReadOnlyBroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                        Map<String, Tuple2<String, Integer>> map = broadcastState.get(null);
                        if (MapUtils.isEmpty(map)) {
                            return;
                        }
                        UserBehavior userBehavior = JacksonUtil.json2Bean(value, UserBehavior.class);

                        out.collect(BroadCastStreamOut.builder()
                                .broadcast(map)
                                .userBehavior(userBehavior)
                                .build());

                    }

                    @Override
                    public void processBroadcastElement(Map<String, Tuple2<String, Integer>> value, Context ctx, Collector<BroadCastStreamOut> out) throws Exception {

                        //value就是从MySQL中每隔5是查询出来并广播到状态中的最新数据!
                        //要把最新的数据放到state中
                        BroadcastState<Void, Map<String, Tuple2<String, Integer>>> broadcastState = ctx.getBroadcastState(descriptor);
                        broadcastState.clear();//清空旧数据
                        broadcastState.put(null, value);//放入新数据
                    }
                });

        configStream.print("broad cast end");

        DataStreamSource<Map<String, HiveEntity>> hiveSource = env.addSource(new HiveSource());

        hiveSource.print("hive===");

        configStream.keyBy(t -> t.getUserBehavior().getUserId()).connect(hiveSource.keyBy(Map::keySet))
                .process(new CoProcess());








        DataStream<Tuple2<String, Long>> tupleDS = env.fromElements(
                Tuple2.of("北京", 1L),
                Tuple2.of("上海", 2L),
                Tuple2.of("北京", 6L),
                Tuple2.of("上海", 8L),
                Tuple2.of("北京", 3L),
                Tuple2.of("上海", 4L)
        );

        ConnectedStreams<BroadCastStreamOut, Tuple2<String, Long>> connect = configStream.keyBy(t -> t.getUserBehavior().getUserId()).connect(tupleDS.keyBy(t -> t.f0));

        // connect.process()
        // configStream.keyBy(t -> t.getUserBehavior().getUserId()).connect(tupleDS.keyBy(t -> t.f0))
        //         .map()




        //TODO 2.transformation
        //需求:求各个城市的value最大值
        //实际中使用maxBy即可
        DataStream<Tuple2<String, Long>> result1 = tupleDS.keyBy(t -> t.f0).maxBy(1);

        //学习时可以使用KeyState中的ValueState来实现maxBy的底层
        DataStream<Tuple3<String, Long, Long>> result2 = tupleDS.keyBy(t -> t.f0)
                .map(new RichMapFunction<Tuple2<String, Long>, Tuple3<String, Long, Long>>() {
                    //-1.定义一个状态用来存放最大值
                    private ValueState<Long> maxValueState;
                    private transient MapState<Long, RowData> leftState;

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

    public static class MySQLSource extends RichParallelSourceFunction<Map<String, Tuple2<String, Integer>>> {
        private boolean flag = true;
        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://node3:3306/bigdata", "root", "123456");
            String sql = "select `user_id`, `metric` from `metric_info`";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<Map<String, Tuple2<String, Integer>>> ctx) throws Exception {
            while (flag) {

                Map<String, Tuple2<String, Integer>> map = new HashMap<>();
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String userId = rs.getString("user_id");
                    int metric = rs.getInt("metric");
                    //Map<String, Tuple2<String, Integer>>
                    map.put(userId, Tuple2.of(userId, metric));
                }
                ctx.collect(map);
                Thread.sleep(5000);//每隔5s更新一下用户的配置信息!
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
            if (rs != null) rs.close();
        }
    }

    public static class HiveSource extends RichParallelSourceFunction<Map<String, HiveEntity>> {
        private boolean flag = true;
        private Connection conn = null;
        private PreparedStatement ps = null;
        private ResultSet rs = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            conn = DriverManager.getConnection("jdbc:mysql://node3:3306/bigdata", "root", "123456");
            String sql = "select `user_id`, `metric`,`pt_d` from `t_hive`";
            ps = conn.prepareStatement(sql);
        }

        @Override
        public void run(SourceContext<Map<String, HiveEntity>> ctx) throws Exception {
            while (flag) {

                Map<String, HiveEntity> map = new HashMap<>();
                ResultSet rs = ps.executeQuery();
                while (rs.next()) {
                    String userId = rs.getString("user_id");
                    int metric = rs.getInt("metric");
                    String ptD = rs.getString("pt_d");
                    //Map<String, Tuple2<String, Integer>>
                    map.put(userId, new HiveEntity(userId, metric, ptD));
                }
                ctx.collect(map);
                Thread.sleep(5000);//每隔5s更新一下用户的配置信息!
            }
        }

        @Override
        public void cancel() {
            flag = false;
        }

        @Override
        public void close() throws Exception {
            if (conn != null) conn.close();
            if (ps != null) ps.close();
            if (rs != null) rs.close();
        }
    }

}