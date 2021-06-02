package com.jsy.aaa.udf;

import org.apache.flink.table.functions.AggregateFunction;

/**
 * @Author: jsy
 * @Date: 2021/5/25 23:55
 */
//自定义一个聚合函数,求每个传感器的平均温度值,保存状态(tempSum,tempCount)
//传入的第一个Double是最终的返回值,这里求的是平均值,所以是Double
//第二个传入的是中间状态存储的值,需要求平均值,那就需要保存所有温度加起来的总温度和温度的数量(多少个),那就是(Double,Int)
// 如果不传AggTempAcc ,那就传入(Double,Int)一样的效果
public class AggTemp extends AggregateFunction<Double, AggTempAcc> {
    @Override
    public Double getValue(AggTempAcc acc) {
        return acc.sum / acc.count;
    }

    @Override
    public AggTempAcc createAccumulator() {
        return new AggTempAcc();
    }

    // 还要实现一个具体的处理计算函数, accumulate(父方法),具体计算的逻辑,
    public void accumulate(AggTempAcc acc, Double temp) {
        acc.sum += temp;
        acc.count += 1;
    }
}

//定义一个类,存储聚合状态,如果不设置,在AggregateFunction 传入的第二个值就是(Double, Int)   温度的总数和温度的数量
class AggTempAcc {
    Double sum = 0.0;
    Integer count = 0;
}
