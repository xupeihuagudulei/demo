package com.jsy.aaa.udf;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @Author: jsy
 * @Date: 2021/5/6 23:32
 *
 * https://blog.51cto.com/mapengfei/2572888
 */
public class UseUdfTest {

    private static String stringLengthUdf = "CREATE FUNCTION stringLengthUdf AS 'com.jsy.aaa.udf.StringLengthUdf'";

    private static String sls_stream = "create table sls_stream(\n" +
            "    a int,\n" +
            "    b int,\n" +
            "    c varchar\n" +
            ") with (\n" +
            "    type='sls',\n" +
            "    endPoint='<yourEndpoint>',\n" +
            "    accessKeyId='<yourAccessId>',\n" +
            "    accessKeySecret='<yourAccessSecret>',\n" +
            "    startTime = '2017-07-04 00:00:00',\n" +
            "    project='<yourProjectName>',\n" +
            "    logStore='<yourLogStoreName>',\n" +
            "    consumerGroup='consumerGroupTest1'\n" +
            ")";

    private static String rds_output_create = "create table rds_output(\n" +
            "    id int,\n" +
            "    len bigint,\n" +
            "    content VARCHAR\n" +
            ") with (\n" +
            "    type='rds',\n" +
            "    url='yourDatabaseURL',\n" +
            "    tableName='<yourDatabaseTableName>',\n" +
            "    userName='<yourDatabaseUserName>',\n" +
            "    password='<yourDatabasePassword>'\n" +
            ")";

    private static String rds_output = "insert into rds_output\n" +
            "select\n" +
            "    a,\n" +
            "    stringLengthUdf(c),\n" +
            "    c as content\n" +
            "from sls_stream";

    public static void main(String[] args) throws Exception {
        //TODO 0.env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env, settings);

        tenv.executeSql(stringLengthUdf);
        // tenv.executeSql(sls_stream);
        // tenv.executeSql(rds_output_create);
        // tenv.executeSql(rds_output);

        //TODO 1.source
        DataStream<UseUdfTest.WC> wordsDS = env.fromElements(
                new UseUdfTest.WC("Hello", 1),
                new UseUdfTest.WC("World", 1),
                new UseUdfTest.WC("HelloWorld", 1)
        );

        //TODO 2.transformation
        //将DataStream转为View或Table
        tenv.createTemporaryView("t_words", wordsDS, $("word"), $("frequency"));

        // todo 这有个坑,不用cast会报错
        // https://blog.csdn.net/wuxuyang_7788/article/details/103636950
        String sql = "select word,sum(frequency) as frequency, cast(stringLengthUdf(word)  as integer) as length\n " +
                "from t_words\n " +
                "group by word";

        //执行sql
        Table resultTable = tenv.sqlQuery(sql);

        /**
         * 表作为流式查询的结果，是动态更新的。所以，将这种动态查询转换成的数据流，同样需要对表的更新操作进行编码，进而有不同的转换模式。
         * Table API中表到DataStream有两种模式：
         *
         * l 追加模式（Append Mode）
         * 用于表只会被插入（Insert）操作更改的场景。
         *
         * l 撤回模式（Retract Mode）
         * 用于任何场景。有些类似于更新模式中Retract模式，它只有Insert和Delete两类操作。
         * 得到的数据会增加一个Boolean类型的标识位（返回的第一个字段），用它来表示到底是新增的数据（Insert），还是被删除的数据（老数据， Delete）。
         *
         * count() group by时，必须使用缩进模式。
         */
        //转为DataStream
        tenv.toRetractStream(resultTable, UseUdfTest.WCWithLength.class).print();// 有聚合，肯定有更新，不能用append

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

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    public static class WCWithLength {
        public String word;
        // 频率
        public long frequency;

        public Integer length;

    }
}

