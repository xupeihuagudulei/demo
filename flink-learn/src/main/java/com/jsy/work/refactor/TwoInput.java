package com.jsy.work.refactor;

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.api.operators.Triggerable;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;

/**
 * @Author: jsy
 * @Date: 2021/8/8 18:14
 */
public class TwoInput extends AbstractStreamOperator<RowData>
        implements TwoInputStreamOperator<RowData, RowData, RowData>,
        Triggerable<Object, VoidNamespace> {

    @Override
    public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {

    }

    @Override
    public void onProcessingTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {

    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {

    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {

    }
}
