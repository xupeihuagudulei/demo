package com.jsy.highfeature;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * flink cdc 是阿里开发的一个组件，可以连接关系型数据库，可以读取全量数据和增量数据
 * Flink CDC是内置了Debezium工具
 * https://github.com/ververica/flink-cdc-connectors
 * https://ververica.github.io/flink-cdc-connectors/master/
 * https://mp.weixin.qq.com/s/iwY5975XXp7QOBeV0q4TfQ
 * <p>
 * 要求：当前MySQL库开启binlog
 * <p>
 * Dynamic Table & ChangeLog Stream
 * Dynamic Table 就是 Flink SQL 定义的动态表，动态表和流的概念是对等的。参照上图，流可以转换成动态表，动态表也可以转换成流。
 * 在 Flink SQL中，数据在从一个算子流向另外一个算子时都是以 Changelog Stream 的形式，任意时刻的 Changelog Stream 可以翻译为一个表，也可以翻译为一个流。
 * <p>
 * stream api
 * 1. 1.12 1.13
 * 2. 支持多表
 * 3. 可以自定义反序列化器
 * table api
 * 1. 1.13
 * 2. 单表
 * 3. 自带反序列化器
 *
 * 2.0
 * https://www.bilibili.com/video/BV1wL4y1Y7Xu?p=15
 * 核心要解决三个问题，即支持无锁、水平扩展、checkpoint。
 * 详解：
 * 对于有主键的表做初始化模式，整体流程主要分为5个阶段
 * 1、Chunk切分    找到最大的id，根据scan.incremental.snapshot.chunk.size：8096 做分页，每个chunk可以单独做ckp
 * 2、Chunk分配（实现并行读取数据&ckp）  全量
 * 3、Chunk读取（支持无锁读取）
 * 4、Chunk汇报  多个chunk保证数据一致性：每一个SourceReader读取完做数据上报给SourceEnumerator，上报完整个任务开启一个binlog读取
 * 5、Chunk分配  增量
 *
 * 将表根据主键拆分很多Chunk，比如主键1-10切分一个chunk，给一个source读取，失败可以只重启这一个chunk
 * 通过记录高位点来保证一致性
 *
 *
 * @Author: jsy
 * @Date: 2021/8/30 12:59
 */
public class FlinkCDC_DataStream {
    public static void main(String[] args) throws Exception {

        SourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("node3")
                .port(3306)
                .databaseList("bigdata") // monitor all tables under inventory database
                .username("root")
                .password("123456")
                .tableList("bigdata.user_info") // 不设置会监听所有表
                // .deserializer(new StringDebeziumDeserializationSchema()) // converts SourceRecord to String
                .deserializer(new FlinkCDCCustomDeserializationSchema()) // converts SourceRecord to String
                .startupOptions(StartupOptions.initial())
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        /*
        // 设置ckp
        env.setParallelism(1);
        env.getCheckpointConfig().setCheckpointTimeout(10000);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 开启状态后端
        if (SystemUtils.IS_OS_WINDOWS) {
            env.setStateBackend(new FsStateBackend("file:///D:/ckp-cdc"));
        } else {
            env.setStateBackend(new FsStateBackend("hdfs://node1:8020/flink-checkpoint/checkpoint-cdc"));
        }
        */

        env
                .addSource(sourceFunction)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering

        env.execute();

    }

}

/*
# 启动yarn session
/export/server/flink/bin/yarn-session.sh -n 2 -tm 800 -s 1 -d

# 另起一个窗口  运行job-会自动执行Checkpoint
/export/server/flink/bin/flink run --class com.jsy.highfeature.FlinkCDC_DataStream /root/jar/flink-cdc.jar
* */

/*
vi /etc/my.cnf
# 随机指定一个不能和其他集群中机器重名的字符串，如果只有一台机器，那就可以随便指定了
server_id=1
log_bin=mysql-bin
binlog_format=ROW


# 重启mysql
service mysql restart

# 查看语句 关注log_bin
show variables like '%log_bin%';
# 查看当前正在写入的binlog文件
show master status;
# 查看当前正在写入的日志文件中的binlog事件（看不出具体内容，只能看个大概）
show binlog events;

https://www.cnblogs.com/grey-wolf/p/10437811.html


* */


/*
op 字段代表操作类型
初始化 op=read
SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={ts_sec=1630311819, file=mysql-bin.000001,
pos=154, snapshot=true}} ConnectRecord{topic='mysql_binlog_source.bigdata.t_student', kafkaPartition=null, key=Struct{id=6},
keySchema=Schema{mysql_binlog_source.bigdata.t_student.Key:STRUCT}, value=Struct{after=Struct{id=6,name=rose,age=20},
source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1630311819316,snapshot=true,db=bigdata,
table=t_student,server_id=0,file=mysql-bin.000001,pos=154,row=0},op=r,ts_ms=1630311819316},
valueSchema=Schema{mysql_binlog_source.bigdata.t_student.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}


insert
SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1630311928,
file=mysql-bin.000001, pos=219, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.bigdata.user_info',
kafkaPartition=null, key=Struct{userID=user_5}, keySchema=Schema{mysql_binlog_source.bigdata.user_info.Key:STRUCT},
value=Struct{after=Struct{userID=user_5,userName=赵五,userAge=40},source=Struct{version=1.5.2.Final,connector=mysql,
name=mysql_binlog_source,ts_ms=1630311928000,db=bigdata,table=user_info,server_id=1,file=mysql-bin.000001,pos=355,row=0},
op=c,ts_ms=1630311927900}, valueSchema=Schema{mysql_binlog_source.bigdata.user_info.Envelope:STRUCT}, timestamp=null,
headers=ConnectHeaders(headers=)}

delete
SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1630311959,
file=mysql-bin.000001, pos=550, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.bigdata.user_info',
kafkaPartition=null, key=Struct{userID=user_5}, keySchema=Schema{mysql_binlog_source.bigdata.user_info.Key:STRUCT},
value=Struct{before=Struct{userID=user_5,userName=赵五,userAge=40},source=Struct{version=1.5.2.Final,connector=mysql,
name=mysql_binlog_source,ts_ms=1630311959000,db=bigdata,table=user_info,server_id=1,file=mysql-bin.000001,pos=686,row=0},
op=d,ts_ms=1630311959010}, valueSchema=Schema{mysql_binlog_source.bigdata.user_info.Envelope:STRUCT}, timestamp=null,
headers=ConnectHeaders(headers=)}

update  40->50
SourceRecord{sourcePartition={server=mysql_binlog_source}, sourceOffset={transaction_id=null, ts_sec=1630311972,
file=mysql-bin.000001, pos=881, row=1, server_id=1, event=2}} ConnectRecord{topic='mysql_binlog_source.bigdata.user_info',
kafkaPartition=null, key=Struct{userID=user_4}, keySchema=Schema{mysql_binlog_source.bigdata.user_info.Key:STRUCT},
value=Struct{before=Struct{userID=user_4,userName=赵六,userAge=40},after=Struct{userID=user_4,userName=赵六,userAge=50},
source=Struct{version=1.5.2.Final,connector=mysql,name=mysql_binlog_source,ts_ms=1630311972000,db=bigdata,table=user_info,
server_id=1,file=mysql-bin.000001,pos=1017,row=0},op=u,ts_ms=1630311972201},
valueSchema=Schema{mysql_binlog_source.bigdata.user_info.Envelope:STRUCT}, timestamp=null, headers=ConnectHeaders(headers=)}




* */


/*
源码
chunk 并行
binlog 单并行度
// 初始化读取 注意lowWatermark highWatermark
com.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader
com.ververica.cdc.connectors.mysql.debezium.reader.SnapshotSplitReader.pollSplitRecords // 全量

// 读取binlog
com.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader

// 单并行度读取binlog
com.ververica.cdc.connectors.mysql.source.assigners.MySqlHybridSplitAssigner.createBinlogSplit

全量读取会将当前chunk数据+binlog从地位点到高位点的数据
com.ververica.cdc.connectors.mysql.debezium.reader.BinlogSplitReader.shouldEmit

*
* */