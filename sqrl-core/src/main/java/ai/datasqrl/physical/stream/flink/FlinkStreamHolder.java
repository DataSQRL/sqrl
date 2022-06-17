package ai.datasqrl.physical.stream.flink;

import ai.datasqrl.physical.stream.FunctionWithError;
import ai.datasqrl.physical.stream.StreamHolder;
import lombok.Value;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

@Value
public class FlinkStreamHolder<T> implements StreamHolder<T> {

    private final FlinkStreamBuilder builder;
    private final DataStream<T> stream;

    @Override
    public <R> StreamHolder<R> mapWithError(FunctionWithError<T, R> function, String errorName, Class<R> clazz) {
        final OutputTag<MapWithErrorProcess.Error> errorTag = new OutputTag<>(
                FlinkStreamEngine.getFlinkName(MapWithErrorProcess.ERROR_TAG_PREFIX, errorName)) {
        };
        return wrap(stream.process(new MapWithErrorProcess<>(errorTag,function), TypeInformation.of(clazz)));
    }

    @Override
    public void printSink() {
        stream.addSink(new PrintSinkFunction<>());
    }

    private<R> FlinkStreamHolder<R> wrap(DataStream<R> newStream) {
        return new FlinkStreamHolder<>(builder,newStream);
    }
}
