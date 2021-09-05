package com.jsy.highfeature;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.List;
import java.util.Objects;

/**
 * 自定义反序列化器
 *
 * @Author: jsy
 * @Date: 2021/8/30 19:20
 */
public class FlinkCDCCustomDeserializationSchema implements DebeziumDeserializationSchema<String> {

    // 源码中collector可以帮助我们数据输出
    @Override
    public void deserialize(SourceRecord sourceRecord, Collector<String> collector) throws Exception {

        // 自定义数据格式
        JSONObject result = new JSONObject();
        String[] split = sourceRecord.topic().split("\\.");
        result.put("db", split[1]);
        result.put("table", split[2]);

        Struct value = (Struct) sourceRecord.value();
        // 获取before数据
        Struct before = value.getStruct("before");
        JSONObject beforeJson = new JSONObject();
        if (Objects.nonNull(before)) {
            List<Field> fields = before.schema().fields();
            for (Field field : fields) {
                beforeJson.put(field.name(), before.get(field));
            }
        }
        result.put("before", beforeJson);

        // 获取after数据
        Struct after = value.getStruct("after");
        JSONObject afterJson = new JSONObject();
        if (Objects.nonNull(after)) {
            List<Field> fields = after.schema().fields();
            for (Field field : fields) {
                afterJson.put(field.name(), after.get(field));
            }
        }
        result.put("after", afterJson);

        Envelope.Operation operation = Envelope.operationFor(sourceRecord);
        result.put("op", operation);

        // 输出
        collector.collect(result.toJSONString());

    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
