package com.jsy.sql

import org.apache.flink.api.java.tuple.Tuple2
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableResult}
import org.apache.flink.types.Row

import java.lang

/**
 * 嵌套json解析成表
 * https://zhuanlan.zhihu.com/p/344961806
 *
 * 目前，JSON 模式总是从表模式派生。目前还不支持显式定义 JSON 模式。Flink JSON 格式使用 jackson databind API
 * 来解析和生成JSON 字符串。下表列出了从 Flink 类型到 JSON 类型的映射。
 *
 * 注意事项:
 * Json 中的每个 {} 都需要用 Row 类型来表示
 * Json 中的每个 [] 都需要用 Array 类型来表示
 * 数组的下标是从 1 开始的不是 0 如上面 SQL 中的 data.snapshots[1].url
 * 关键字在任何地方都需要加反引号 如上面 SQL 中的 `type`
 * select 语句中的字段类型和顺序一定要和结果表的字段类型和顺序保持一致
 * UDF 可以直接在建表语句中使用
 *
 * @Author: jsy
 * @Date: 2021/7/6 7:13 
 */

/*
/export/server/kafka/bin/kafka-console-producer.sh --broker-list node1:9092 --topic input_kafka
# 消息
{"funcName":"test","data":{"snapshots":[{"content_type":"application/x-gzip-compressed-jpeg","url":"https://blog.csdn.net/xianpanjia4616"}],"audio":[{"content_type":"audio/wav","url":" https://bss.csdn.net/m/topic/blog_star2020/detail?username=xianpanjia4616"}]},"resultMap":{"result":{"cover":"/data/test/log.txt"},"isSuccess":true},"meta":{"video_type":"normal"},"type":2,"timestamp":1610549997263,"arr":[{"address":"北京市海淀区","city":"beijing"},{"address":"北京市海淀区","city":"beijing"},{"address":"北京市海淀区","city":"beijing"}],"map":{"flink":456},"doublemap":{"inner_map":{"key":123}}}

{
    "funcName": "test",
    "data": {
        "snapshots": [
            {
                "content_type": "application/x-gzip-compressed-jpeg",
                "url": "https://blog.csdn.net/xianpanjia4616"
            }
        ],
        "audio": [
            {
                "content_type": "audio/wav",
                "url": " https://bss.csdn.net/m/topic/blog_star2020/detail?username=xianpanjia4616"
            }
        ]
    },
    "resultMap": {
        "result": {
            "cover": "/data/test/log.txt"
        },
        "isSuccess": true
    },
    "meta": {
        "video_type": "normal"
    },
    "type": 2,
    "timestamp": 1610549997263,
    "arr": [
        {
            "address": "北京市海淀区",
            "city": "beijing"
        },
        {
            "address": "北京市海淀区",
            "city": "beijing"
        },
        {
            "address": "北京市海淀区",
            "city": "beijing"
        }
    ],
    "map": {
        "flink": 456
    },
    "doublemap": {
        "inner_map": {
            "key": 123
        }
    }
}
*/
object NestedJsonRead2 {

  def main(args: Array[String]): Unit = {
    //TODO 0.env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val settings: EnvironmentSettings = EnvironmentSettings.newInstance.useBlinkPlanner.inStreamingMode.build
    val tenv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    //TODO 1.source
    // 直接连接kafka，获取的就是一个表
    val inputTable: TableResult = tenv.executeSql(
      """
        |CREATE TABLE kafka_source (
        |    funcName STRING,
        |    data ROW<snapshots ARRAY<ROW<content_type STRING,url STRING>>,audio ARRAY<ROW<content_type STRING,url STRING>>>,
        |    resultMap ROW<`result` MAP<STRING,STRING>,isSuccess BOOLEAN>,
        |    meta  MAP<STRING,STRING>,
        |    `type` INT,
        |    `timestamp` BIGINT,
        |    arr ARRAY<ROW<address STRING,city STRING>>,
        |    map MAP<STRING,INT>,
        |    doublemap MAP<STRING,MAP<STRING,INT>>,
        |    proctime as PROCTIME()
        |) WITH (
        |    'connector' = 'kafka', -- 使用 kafka connector
        |    'topic' = 'input_kafka',  -- kafka topic
        |    'properties.bootstrap.servers' = 'node1:9092',  -- broker连接信息
        |    'properties.group.id' = 'jason_flink_test', -- 消费kafka的group_id
        |    'scan.startup.mode' = 'latest-offset',  -- 读取数据的位置
        |    'format' = 'json',  -- 数据源格式为 json
        |    'json.fail-on-missing-field' = 'true', -- 字段丢失任务不失败
        |    'json.ignore-parse-errors' = 'false'  -- 解析失败跳过
        |)
        |""".stripMargin)

    //TODO 2.transformation
    // 解析sql
    val etlResult: Table = tenv.sqlQuery(
      """
        |select
        |funcName,
        |doublemap['inner_map']['key'],
        |count(data.snapshots[1].url),
        |`type`,
        |TUMBLE_START(proctime, INTERVAL '30' second) as t_start
        |from kafka_source
        |group by TUMBLE(proctime, INTERVAL '30' second),funcName,`type`,doublemap['inner_map']['key']
        |
        |
        |""".stripMargin)

    etlResult.printSchema()

    //TODO 3.sink
    val resultDS: DataStream[Tuple2[lang.Boolean, Row]] = tenv.toRetractStream(etlResult, classOf[Row])
    resultDS.print

    //TODO 4.execute
    env.execute

  }

}
