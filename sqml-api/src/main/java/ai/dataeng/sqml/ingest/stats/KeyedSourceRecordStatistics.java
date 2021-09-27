package ai.dataeng.sqml.ingest.stats;

import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.ingest.DatasetRegistration;
import ai.dataeng.sqml.ingest.source.SourceRecord;
import ai.dataeng.sqml.schema2.basic.ConversionError;
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
    private final OutputTag<SourceTableStatistics> statsOutput;
    private final DatasetRegistration datasetReg;

    private transient ValueState<SourceTableStatistics> stats;
    private transient ValueState<Long> nextTimer;

    public KeyedSourceRecordStatistics(OutputTag<SourceTableStatistics> tag, DatasetRegistration datasetReg) {
        this.statsOutput = tag;
        this.datasetReg = datasetReg;
    }

    @Override
    public void processElement(SourceRecord sourceRecord, Context context, Collector<SourceRecord> out) throws Exception {
        SourceTableStatistics acc = stats.value();
        if (acc == null) {
            acc = new SourceTableStatistics(datasetReg);
            long timer = FlinkUtilities.getCurrentProcessingTime() + TimeUnit.MINUTES.toMillis(maxTimeInMin);
            nextTimer.update(timer);
            context.timerService().registerProcessingTimeTimer(timer);
            //Register an event timer into the far future to trigger when the stream ends
            context.timerService().registerEventTimeTimer(Long.MAX_VALUE);
        }
        ConversionError.Bundle<StatsIngestError> errors = acc.validate(sourceRecord);
        if (errors.isFatal()) {
            //TODO: Record is flawed, put it in sideoutput and issue warning
        } else {
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
    }

    @Override
    public void onTimer(long timestamp, OnTimerContext ctx, Collector<SourceRecord> out) throws Exception {
        SourceTableStatistics acc = stats.value();
        if (acc != null) ctx.output(statsOutput, acc);
        stats.clear();
        nextTimer.clear();
    }

    @Override
    public void open(Configuration config) {
        ValueStateDescriptor<SourceTableStatistics> statsDesc =
                new ValueStateDescriptor<>(statsOutput.getId() + STATE_NAME_SUFFIX + ".data",
                        TypeInformation.of(new TypeHint<SourceTableStatistics>() {
                        }));
        stats = getRuntimeContext().getState(statsDesc);
        ValueStateDescriptor<Long> nextTimerDesc =
                new ValueStateDescriptor<>(statsOutput.getId() + STATE_NAME_SUFFIX + ".timer", Long.class);
        nextTimer = getRuntimeContext().getState(nextTimerDesc);
    }

}
