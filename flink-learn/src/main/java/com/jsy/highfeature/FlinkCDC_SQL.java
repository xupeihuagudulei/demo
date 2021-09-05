package com.jsy.highfeature;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * flink cdc 是阿里开发的一个组件，可以连接关系型数据库，可以读取全量数据和增量数据
 * https://github.com/ververica/flink-cdc-connectors
 *
 * sql 模式要求 flink1.13+
 *
 * @Author: jsy
 * @Date: 2021/8/30 12:59
 */
public class FlinkCDC_SQL {

    private static final String GENERAL_PRINT_SINK_SQL = "CREATE TABLE print_table WITH ('connector' = 'print')\n" +
            "LIKE user_info (EXCLUDING ALL)";

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tenv = StreamTableEnvironment.create(env);

        // 反序列化器没指定，表格会自动解析
        tenv.executeSql("CREATE TABLE user_info (\n" +
                " userID STRING NOT NULL,\n" +
                " userName STRING,\n" +
                " userAge STRING,\n" +
                " PRIMARY KEY (`userID`) NOT ENFORCED\n"+
                ") WITH (\n" +
                " 'connector' = 'mysql-cdc',\n" +
                " 'scan.startup.mode' = 'latest-offset',\n" +
                " 'hostname' = 'node3',\n" +
                " 'port' = '3306',\n" +
                " 'username' = 'root',\n" +
                " 'password' = '123456',\n" +
                " 'database-name' = 'bigdata',\n" +
                " 'table-name' = 'user_info'\n" +
                ")");

        // Table table = tenv.sqlQuery("select * from user_info");
        // tenv.toRetractStream(table, Row.class).print();

        tenv.executeSql(GENERAL_PRINT_SINK_SQL);
        tenv.executeSql("insert into print_table select * from user_info");




    }

}

