package com.jsy.aaa.udf;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;

/**
 * 默认情况下TableFunction返回值类型是由flink类型抽取工具决定。对于基础类型及简单的POJOS是足够的，
 * 但是更复杂的类型，自定义类型，组合类型，会报错。这种情况下，返回值类型的TypeInformation，
 * 需要手动指定，方法是重载TableFunction#getResultType()。
 *
 * @Author: jsy
 * @Date: 2021/5/24 7:23
 */
public class CustomTypeSplit extends TableFunction<Row> {

    private String separator = " ";

    public CustomTypeSplit(String separator) {
        this.separator = separator;
    }

    public void eval(String str) {
        for (String s : str.split(separator)) {
            Row row = new Row(2);
            row.setField(0, s);
            row.setField(1, s.length());
            collect(row);
            // collect(new Tuple2<String, Integer>(s, s.length()));
        }
    }

    @Override
    public TypeInformation<Row> getResultType() {
        return Types.ROW(Types.STRING, Types.INT);
    }
}
