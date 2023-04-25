package com.datasqrl.config;

import com.datasqrl.io.util.TimeAnnotatedRecord;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;

public interface DataStreamSourceFactory extends FlinkSourceFactory<SingleOutputStreamOperator<TimeAnnotatedRecord<String>>> {

}