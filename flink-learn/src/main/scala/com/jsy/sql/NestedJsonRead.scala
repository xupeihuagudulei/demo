package com.jsy.sql

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.types.Row

import java.lang

/**
 * 嵌套json解析成表
 * https://www.jianshu.com/p/848a399d977e
 * https://blog.csdn.net/YouLoveItY/article/details/108276799
 *
 * @Author: jsy
 * @Date: 2021/7/6 7:13 
 */

/*
/export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic input_kafka
# 消息
{"data":{"data":{"mac_value":0,"ad_name":2056,"voice":75,"dataTimeStamp":1598522106830},"type":3,"deviceId":"001C92F7DCd85A"},"timestamp":1598522106835,"type":1}

{
    "data":{
        "data":{
            "mac_value":0,
            "ad_name":2056,
            "voice":75,
            "dataTimeStamp":1598522106830
        },
        "type":3,
        "deviceId":"001C92F7DCd85A"
    },
    "timestamp":1598522106835,
    "type":1
}
*/
object NestedJsonRead {

  def main(args: Array[String]): Unit = {
    //TODO 0.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //TODO 1.source
    // 直接连接kafka，获取的就是一个表
    val inputTable: TableResult = tenv.executeSql(
      """
        |CREATE TABLE t_1144_1 (
        |   -- 最主要的是这行,定义类型为ROW
        |   `data` ROW(deviceId string,`data` ROW(mac_value string,ad_name string,voice string,dataTimeStamp string)),
        |   -- 通过xx.xx取数据,映射成自己想要的表
        |    deviceId as cast(`data`.deviceId as varchar) ,
        |    mac_value as cast( `data`.`data`.mac_value as integer),
        |    ad_name as cast( `data`.`data`.ad_name as integer),
        |    voice as cast( `data`.`data`.voice as integer),
        |    eventTime  as cast( `data`.`data`.`dataTimeStamp` as varchar),
        |     -- 时间窗口以及水位线
        |    windowEventTime AS TO_TIMESTAMP(FROM_UNIXTIME((cast(cast( `data`.`data`.`dataTimeStamp` as varchar) as bigint)+43200000)/1000)),
        |    WATERMARK FOR windowEventTime AS windowEventTime - INTERVAL '2' SECOND
        |) WITH (
        |    'connector' = 'kafka',
        |    'topic' = 'input_kafka',
        |    'properties.bootstrap.servers' = 'node1:9092',
        |    'properties.group.id' = 'testGroup',
        |    'scan.startup.mode' = 'latest-offset',
        |    'format' = 'json'
        |)
        |""".stripMargin)

    //TODO 2.transformation
    val sql = "select * from t_1144_1"
    val etlResult: Table = tenv.sqlQuery(sql)

    etlResult.printSchema()

    //TODO 3.sink
    val resultDS: DataStream[Tuple2[lang.Boolean, Row]] = tenv.toRetractStream(etlResult, classOf[Row])
    resultDS.print

    //val outputTable: TableResult = tenv.executeSql(
    //  """
    //    |CREATE TABLE print_table WITH ('connector' = 'print') LIKE t_1144_1 (EXCLUDING ALL)
    //    |""".stripMargin)
    //
    //tenv.executeSql("insert into print_table select * from t_1144_1" + etlResult)

    //TODO 4.execute
    env.execute

  }

}
