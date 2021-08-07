package com.jsy.highfeature;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.catalog.hive.HiveCatalog;

/**
 * 验证成功
 *
 * Flink从1.9开始支持集成Hive，不过1.9版本为beta版，不推荐在生产环境中使用。
 * 在Flink1.10版本中，标志着对 Blink的整合宣告完成。
 * 值得注意的是，不同版本的Flink对于Hive的集成有所差异，接下来将以最新的Flink1.12版本为例，实现Flink集成Hive
 *
 * https://ci.apache.org/projects/flink/flink-docs-release-1.12/dev/table/connectors/hive/
 * https://zhuanlan.zhihu.com/p/338506408
 *
 * @Author: jsy
 * @Date: 2021/4/18 20:36
 */

/*
/export/server/hive/bin/beeline -u jdbc:hive2://node3:10000 -n node3

insert into person values("1","lisi","20");

* */
public class HiveDemo {
    public static void main(String[] args){
        //TODO 0.env
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        //TODO 指定hive的配置
        String name            = "myhive";
        String defaultDatabase = "default";
        String hiveConfDir = "./conf";

        //TODO 根据配置创建hiveCatalog  catalog是目录的意思，可以理解为数据库
        HiveCatalog hive = new HiveCatalog(name, defaultDatabase, hiveConfDir);
        //注册catalog
        tableEnv.registerCatalog("myhive", hive);
        //使用注册的catalog
        tableEnv.useCatalog("myhive");

        //向Hive表中写入数据
        String insertSQL = "insert into person select * from person";
        TableResult result = tableEnv.executeSql(insertSQL);

        System.out.println(result.getJobClient().get().getJobStatus());
    }
}
