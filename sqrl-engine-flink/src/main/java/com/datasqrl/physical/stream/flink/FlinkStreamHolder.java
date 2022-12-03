package com.datasqrl.physical.stream.flink;

import com.datasqrl.physical.stream.FunctionWithError;
import com.datasqrl.physical.stream.StreamHolder;
import lombok.Value;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

@Value
public class FlinkStreamHolder<T> implements StreamHolder<T> {

    private final FlinkStreamEngine.Builder builder;
    private final DataStream<T> stream;

    @Override
    public <R> StreamHolder<R> mapWithError(FunctionWithError<T, R> function, String errorName, Class<R> clazz) {
        final OutputTag<ProcessError> errorTag = builder.getErrorTag(errorName);
        SingleOutputStreamOperator<R> result = stream.process(new MapWithErrorProcess<>(errorTag,function), TypeInformation.of(clazz));
        //TODO: result.getSideOutput(errorTag).addSink(Do something with the error)
        return wrap(result);
    }

    @Override
    public void printSink() {
        stream.addSink(new PrintSinkFunction<>());
    }

    private<R> FlinkStreamHolder<R> wrap(DataStream<R> newStream) {
        return new FlinkStreamHolder<>(builder,newStream);
    }
}
