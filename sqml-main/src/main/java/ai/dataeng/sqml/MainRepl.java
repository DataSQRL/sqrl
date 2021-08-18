package ai.dataeng.sqml;

import ai.dataeng.sqml.flink.util.BufferedLatestSelector;
import ai.dataeng.sqml.flink.util.FlinkUtilities;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class MainRepl {


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment flinkEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        flinkEnv.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        FlinkUtilities.enableCheckpointing(flinkEnv);

        List<Integer> input = Arrays.asList(new Integer[]{1,2,3,4,5});

        DataStream<Integer> data = flinkEnv.fromCollection(input);

        final OutputTag<LongCounter> statsOutput = new OutputTag<>("sideoutput"){};

        SingleOutputStreamOperator<Integer> process = data.keyBy(FlinkUtilities.getHashPartitioner(6))
                .process(new KeyedSourceRecordStatistics(statsOutput));
        process.addSink(new PrintSinkFunction<>()); //TODO: persist last 100 for querying

        //Process the gathered statistics in the side output
        final int randomKey = 1;
        process.getSideOutput(statsOutput)
                .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
                .reduce(
                        new ReduceFunction<LongCounter>() {
                            @Override
                            public LongCounter reduce(LongCounter acc, LongCounter add) throws Exception {
                                acc.merge(add);
                                return acc;
                            }
                        })
                .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
                //BufferedLatestSelector the offending component - if you comment this out it runs normally
                .process(new BufferedLatestSelector("buffer",
                        500, LongCounter.class), TypeInformation.of(LongCounter.class))
                .addSink(new PrintSinkFunction<>());
        try {
            flinkEnv.execute("TestJob");
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public static class KeyedSourceRecordStatistics extends KeyedProcessFunction<Integer, Integer, Integer> {

        public static final String STATE_NAME_SUFFIX = "-state";

        private int maxValue = 100000;
        private int maxTimeInMin = 5;
        private OutputTag<LongCounter> statsOutput;

        private transient ValueState<LongCounter> stats;
        private transient ValueState<Long> nextTimer;

        public KeyedSourceRecordStatistics(OutputTag<LongCounter> tag) {
            this.statsOutput = tag;
        }

        @Override
        public void processElement(Integer sourceRecord, Context context, Collector<Integer> out) throws Exception {
            LongCounter acc = stats.value();
            if (acc == null) {
                acc = new LongCounter();
                long timer = FlinkUtilities.getCurrentProcessingTime() + TimeUnit.MINUTES.toMillis(maxTimeInMin);
                nextTimer.update(timer);
                context.timerService().registerProcessingTimeTimer(timer);
                //Register an event timer into the far future to trigger when the stream ends
                context.timerService().registerEventTimeTimer(Long.MAX_VALUE);
            }
            acc.add(sourceRecord);
            stats.update(acc);
            if (acc.getLocalValue() >= maxValue) {
                context.timerService().deleteProcessingTimeTimer(nextTimer.value());
                context.output(statsOutput, acc);
                stats.clear();
                nextTimer.clear();
            }
            out.collect(sourceRecord);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out) throws Exception {
            LongCounter acc = stats.value();
            if (acc != null) ctx.output(statsOutput, acc);
            stats.clear();
            nextTimer.clear();
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<LongCounter> statsDesc =
                    new ValueStateDescriptor<>(statsOutput.getId() + STATE_NAME_SUFFIX + ".data",
                            TypeInformation.of(new TypeHint<LongCounter>() {
                            }));
            stats = getRuntimeContext().getState(statsDesc);
            ValueStateDescriptor<Long> nextTimerDesc =
                    new ValueStateDescriptor<>(statsOutput.getId() + STATE_NAME_SUFFIX + ".timer", Long.class);
            nextTimer = getRuntimeContext().getState(nextTimerDesc);
        }

    }


}
