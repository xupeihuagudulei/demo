package com.jsy.work;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigValue;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * typesafeConfig 工具
 *
 * 第二种：ParameterTool
 *
 * @Author: jsy
 * @Date: 2021/5/26 23:40
 */
public class ParseParam {

    public static void main(String[] args) throws IOException {
        Path path = new Path("E:\\GitHub\\demo\\flink-learn\\src\\main\\resources\\kafka2.conf");
        FileSystem fileSystem = path.getFileSystem();
        FSDataInputStream open = fileSystem.open(path);
        InputStreamReader inputStreamReader = new InputStreamReader(open);

        Config config = ConfigFactory.parseReader(inputStreamReader);

        Set<String> keys = config.entrySet().stream()
                .map(Map.Entry::getKey)
                .map(t -> t.substring(0, t.indexOf(".")))
                .collect(Collectors.toSet());

        for (String key : keys) {

            Config kafkaConf = config.getConfig(key);
            Set<Map.Entry<String, ConfigValue>> entries = kafkaConf.entrySet();
            for (Map.Entry<String, ConfigValue> entry : entries) {

                System.out.println(entry.getKey() + "---------" + entry.getValue().unwrapped());
            }

        }

        // Config aa = config.getConfig("aa");
        // System.out.println(config.getString("aa"));
        //
        // Config bb = config.getConfig("two");
        // System.out.println(bb.getString("two1"));

        /**
         * ConfigFactory.load() 默认加载classpath下的配置文件：
         *
         * 加载顺序为：application.conf --->  application.sjon  ---->  application.properties
         */


        // ConfigFactory.load()


    }
}
