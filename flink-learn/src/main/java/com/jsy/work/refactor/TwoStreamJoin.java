package com.jsy.work.refactor;

import org.apache.flink.runtime.state.VoidNamespace;
import org.apache.flink.streaming.api.operators.InternalTimer;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.temporal.BaseTwoInputStreamOperatorWithStateRetention;

/**
 * @Author: jsy
 * @Date: 2021/8/8 2:51
 */
public class TwoStreamJoin extends BaseTwoInputStreamOperatorWithStateRetention {
    protected TwoStreamJoin(long minRetentionTime, long maxRetentionTime) {
        super(minRetentionTime, maxRetentionTime);
    }

    @Override
    public void cleanupState(long time) {

    }

    @Override
    public void onEventTime(InternalTimer<Object, VoidNamespace> timer) throws Exception {

    }

    @Override
    public void processElement1(StreamRecord<RowData> element) throws Exception {

    }

    @Override
    public void processElement2(StreamRecord<RowData> element) throws Exception {

    }
}
