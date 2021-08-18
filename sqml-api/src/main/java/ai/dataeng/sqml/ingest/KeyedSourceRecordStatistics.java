package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.source.SourceRecord;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.concurrent.TimeUnit;

public class KeyedSourceRecordStatistics extends KeyedProcessFunction<Integer, SourceRecord, SourceRecord> {

    public static final String STATE_NAME_SUFFIX = "-state";

    private int maxRecords = 100000;
    private int maxTimeInMin = 5;
    private OutputTag<SourceTableStatistics.Accumulator> statsOutput;

    private transient ValueState<SourceTableStatistics.Accumulator> stats;
    private transient ValueState<Long> nextTimer;

    public KeyedSourceRecordStatistics(OutputTag<SourceTableStatistics.Accumulator> tag) {
        this.statsOutput = tag;
    }

    @Override
    public void processElement(SourceRecord sourceRecord, Context context, Collector<SourceRecord> out) throws Exception {
        SourceTableStatistics.Accumulator acc = stats.value();
        if (acc == null) {
            acc = new SourceTableStatistics.Accumulator();
            long timer = FlinkUtilities.getCurrentProcessingTime() + TimeUnit.MINUTES.toMillis(maxTimeInMin);
            nextTimer.update(timer);
            context.timerService().registerProcessingTimeTimer(timer);
            //Register an event timer into the far future to trigger when the stream ends
            context.timerService().registerEventTimeTimer(Long.MAX_VALUE);
        }
        acc.add(sourceRecord);
        stats.update(acc);
        if (acc.getCount() >= maxRecords) {
            context.timerService().deleteProcessingTimeTimer(nextTimer.value());
            context.output(statsOutput, acc);
            stats.clear();
            nextTimer.clear();
        }
        out.collect(sourceRecord);
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SourceRecord> out) throws Exception {
        SourceTableStatistics.Accumulator acc = stats.value();
        if (acc != null) ctx.output(statsOutput, acc);
        stats.clear();
        nextTimer.clear();
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<SourceTableStatistics.Accumulator> statsDesc =
                new ValueStateDescriptor<>(statsOutput.getId() + STATE_NAME_SUFFIX + ".data",
                        TypeInformation.of(new TypeHint<SourceTableStatistics.Accumulator>() {
                        }));
        stats = getRuntimeContext().getState(statsDesc);
        ValueStateDescriptor<Long> nextTimerDesc =
                new ValueStateDescriptor<>(statsOutput.getId() + STATE_NAME_SUFFIX + ".timer", Long.class);
        nextTimer = getRuntimeContext().getState(nextTimerDesc);
    }

}
