package com.jsy.connectors;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * Flink-Connectors-三方提供的RedisSink
 *
 * 通过flink 操作redis 其实我们可以通过传统的redis 连接池Jpoools 进行redis 的相关操作，
 * 但是flink提供了专门操作redis 的RedisSink，使用起来更方便，而且不用我们考虑性能的问题，接下来将主要介绍RedisSink如何使用。
 * 不是flink官方的，是第三方提供的
 * https://bahir.apache.org/docs/flink/current/flink-streaming-redis/
 *
 * 需求:
 * 从Socket接收实时流数据,做WordCount,并将结果写入到Redis
 * 数据结构使用:
 * 单词:数量 (key-String, value-String)
 * wcresult: 单词:数量 (key-String, value-Hash)
 * 注意: Redis的Key始终是String, value可以是:String/Hash/List/Set/有序Set
 *
 * @Author: jsy
 * @Date: 2021/3/26 8:39
 */
public class RedisDemo {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);

        //TODO 1.source
        DataStream<String> lines = env.socketTextStream("node1", 9999);

        //TODO 2.transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        }).keyBy(t -> t.f0).sum(1);


        //TODO 3.sink
        result.print();

        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").build();
        RedisSink<Tuple2<String, Integer>> redisSink = new RedisSink<Tuple2<String, Integer>>(conf,new MyRedisMapper());
        result.addSink(redisSink);

        //TODO 4.execute
        env.execute();
    }

    public static class MyRedisMapper implements RedisMapper<Tuple2<String, Integer>> {

        // 设置使用的redis 数据结构类型，和key 的名称，通过RedisCommand 设置数据结构类型
        @Override
        public RedisCommandDescription getCommandDescription() {
            //我们选择的数据结构对应的是 key:String("wcresult"),value:Hash(单词,数量),命令为HSET
            return new RedisCommandDescription(RedisCommand.HSET, "wcresult");
        }

        // 设置value 中的键值对key的值
        @Override
        public String getKeyFromData(Tuple2<String, Integer> t) {
            return t.f0;
        }

        // 设置value 中的键值对value的值
        @Override
        public String getValueFromData(Tuple2<String, Integer> t) {
            return t.f1.toString();
        }
    }
}
