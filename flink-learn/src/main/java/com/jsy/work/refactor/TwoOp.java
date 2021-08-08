package com.jsy.work.refactor;

import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
import org.apache.flink.streaming.api.operators.TwoInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * @Author: jsy
 * @Date: 2021/8/8 18:14
 */
public class TwoOp implements TwoInputStreamOperator {
    @Override
    public void processElement1(StreamRecord element) throws Exception {

    }

    @Override
    public void processElement2(StreamRecord element) throws Exception {

    }

    @Override
    public void processWatermark1(Watermark mark) throws Exception {

    }

    @Override
    public void processWatermark2(Watermark mark) throws Exception {

    }

    @Override
    public void processLatencyMarker1(LatencyMarker latencyMarker) throws Exception {

    }

    @Override
    public void processLatencyMarker2(LatencyMarker latencyMarker) throws Exception {

    }

    @Override
    public void open() throws Exception {

    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public void dispose() throws Exception {

    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {

    }

    @Override
    public OperatorSnapshotFutures snapshotState(long checkpointId, long timestamp, CheckpointOptions checkpointOptions, CheckpointStreamFactory storageLocation) throws Exception {
        return null;
    }

    @Override
    public void initializeState(StreamTaskStateInitializer streamTaskStateManager) throws Exception {

    }

    @Override
    public MetricGroup getMetricGroup() {
        return null;
    }

    @Override
    public OperatorID getOperatorID() {
        return null;
    }

    @Override
    public void setKeyContextElement2(StreamRecord record) throws Exception {

    }

    @Override
    public void setKeyContextElement1(StreamRecord record) throws Exception {

    }

    @Override
    public void notifyCheckpointComplete(long l) throws Exception {

    }

    @Override
    public void setCurrentKey(Object key) {

    }

    @Override
    public Object getCurrentKey() {
        return null;
    }
}
