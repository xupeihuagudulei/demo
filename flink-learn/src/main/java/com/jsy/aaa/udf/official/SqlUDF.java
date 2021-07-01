package com.jsy.aaa.udf.official;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * 官方UDF案例
 *
 * https://help.aliyun.com/knowledge_list/62712.html?spm=a2c4g.11186623.3.6.5e792b74jer5EA
 * --sql 方式
 *
 * @Author: jsy
 * @Date: 2021/4/5 23:29
 */
public class SqlUDF {
    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        //TODO 1.source
        DataStream<WC> wordsDS = env.fromElements(
                new WC("Hello^8^0", 1),
                new WC("World^t^8", 1),
                new WC("Hello^1^4", 1)
        );

        //TODO 2.transformation
        //将DataStream转为View或Table
        tenv.createTemporaryView("t_words", wordsDS, $("word"), $("frequency"));
/*
select word,sum(frequency) as frequency
from t_words
group by word
 */

        // csv 取值 SPLIT_INDEX(word, '^', 1)
        String sql = "select concat(SPLIT_INDEX(word, '^', 1),'=',SPLIT_INDEX(word, '^', 1)) as word,frequency\n " +
                "from t_words ";

        //执行sql
        Table resultTable = tenv.sqlQuery(sql);



        //转为DataStream
        DataStream<Tuple2<Boolean, Row>> resultDS = tenv.toRetractStream(resultTable, Row.class);// 有聚合，肯定有更新，不能用append
        //toAppendStream → 将计算后的数据append到结果DataStream中去
        //toRetractStream  → 将计算后的新的数据在DataStream原数据的基础上更新true或是删除false
        //类似StructuredStreaming中的append/update/complete

        //TODO 3.sink
        resultDS.print();
        //new WC("Hello", 1),
        //new WC("World", 1),
        //new WC("Hello", 1)
        //输出结果
        //(true,Demo02.WC(word=Hello, frequency=1))  -- "Hello", 1
        //(true,Demo02.WC(word=World, frequency=1))  -- "World", 1
        //(false,Demo02.WC(word=Hello, frequency=1))  --false 为删除 "Hello", 1
        //(true,Demo02.WC(word=Hello, frequency=2))   --"Hello", 2

        //TODO 4.execute
        env.execute();
    }

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WC {
        public String word;
        // 频率
        public long frequency;
    }
}
