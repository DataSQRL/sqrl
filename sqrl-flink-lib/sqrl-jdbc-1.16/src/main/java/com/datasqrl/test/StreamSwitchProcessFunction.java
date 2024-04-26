//
//package com.datasqrl.test;
//
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.metrics.groups.OperatorMetricGroup;
//import org.apache.flink.runtime.checkpoint.CheckpointOptions;
//import org.apache.flink.runtime.jobgraph.OperatorID;
//import org.apache.flink.runtime.state.CheckpointStreamFactory;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.datastream.BroadcastStream;
//import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
//import org.apache.flink.streaming.api.operators.OperatorSnapshotFutures;
//import org.apache.flink.streaming.api.operators.StreamTaskStateInitializer;
//import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
//import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
//import org.apache.flink.streaming.runtime.watermarkstatus.WatermarkStatus;
//import org.apache.flink.table.data.RowData;
//import org.apache.flink.util.Collector;
//import org.apache.flink.api.common.state.ValueState;
//import org.apache.flink.api.common.state.ValueStateDescriptor;
//import org.apache.flink.streaming.api.watermark.Watermark;
//
///**
// * A Flink ProcessFunction to switch from processing one DataStream to another.
// * This function assumes that the second DataStream is broadcasted.
// */
//public class StreamSwitchProcessFunction implements
//    OneInputStreamOperator<RowData, RowData> {
//
//    private transient ValueState<Boolean> finishedFileProcessing;
////    private final BroadcastStream<RowData> kafkaStream;
//
//    public StreamSwitchProcessFunction(DataStream<RowData> kafkaStream) {
////        this.kafkaStream = kafkaStream;
//    }
//
////    @Override
////    public void open(Configuration config) {
////        ValueStateDescriptor<Boolean> descriptor = new ValueStateDescriptor<>(
////                "finishedFileProcessing", // the state name
////                Boolean.class,            // type information
////                Boolean.FALSE);           // default value of the state
////        finishedFileProcessing = getRuntimeContext().getState(descriptor);
////    }
//
//    @Override
//    public void processElement(StreamRecord<RowData> streamRecord) throws Exception {
//
//    }
//
//    @Override
//    public void processWatermarkStatus(WatermarkStatus watermarkStatus) throws Exception {
//
//    }
//
//    @Override
//    public void processLatencyMarker(LatencyMarker latencyMarker) throws Exception {
//
//    }
//
//    @Override
//    public void open() throws Exception {
//
//    }
//
//    @Override
//    public void finish() throws Exception {
//
//    }
//
//    @Override
//    public void close() throws Exception {
//
//    }
//
//    @Override
//    public void prepareSnapshotPreBarrier(long l) throws Exception {
//
//    }
//
//    @Override
//    public OperatorSnapshotFutures snapshotState(long l, long l1,
//        CheckpointOptions checkpointOptions,
//        CheckpointStreamFactory checkpointStreamFactory) throws Exception {
//        return null;
//    }
//
//    @Override
//    public void initializeState(StreamTaskStateInitializer streamTaskStateInitializer)
//        throws Exception {
//
//    }
//
//    @Override
//    public void setKeyContextElement1(StreamRecord<?> streamRecord) throws Exception {
//
//    }
//
//    @Override
//    public void setKeyContextElement2(StreamRecord<?> streamRecord) throws Exception {
//
//    }
//
//    @Override
//    public OperatorMetricGroup getMetricGroup() {
//        return null;
//    }
//
//    @Override
//    public OperatorID getOperatorID() {
//        return null;
//    }
//
//    @Override
//    public void notifyCheckpointComplete(long checkpointId) throws Exception {
//
//    }
//
//    @Override
//    public void setCurrentKey(Object o) {
//
//    }
//
//    @Override
//    public Object getCurrentKey() {
//        return null;
//    }
////
////    @Override
////    public void processElement(RowData value, Context ctx, Collector<RowData> out) throws Exception {
////        Boolean finished = finishedFileProcessing.value();
////        if (!finished) {
////            out.collect(value);
////        }
////    }
//
////    @Override
////    public void onTimer(long timestamp, OnTimerContext ctx, Collector<RowData> out) throws Exception {
//        // This method can be used if you have timing control logic
////    }
//
//    @Override
//    public void processWatermark(Watermark mark) throws Exception {
//        if (mark.getTimestamp() == Long.MAX_VALUE) {
////             Assuming that reaching the maximum watermark value signals the end of the file stream
//            finishedFileProcessing.update(Boolean.TRUE);
//        }
//    }
//}
