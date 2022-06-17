package ai.datasqrl.execute.flink.monitor;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.execute.flink.util.FlinkUtilities;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.SourceDataset;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import java.util.concurrent.TimeUnit;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class KeyedSourceRecordStatistics extends
    KeyedProcessFunction<Integer, SourceRecord.Raw, SourceRecord.Raw> {

  public static final String STATE_NAME_SUFFIX = "-state";

  private final int maxRecords = 100000;
  private final int maxTimeInMin = 5;
  private final OutputTag<SourceTableStatistics> statsOutput;
  private final SourceDataset.Digest datasetReg;

  private transient ValueState<SourceTableStatistics> stats;
  private transient ValueState<Long> nextTimer;

  public KeyedSourceRecordStatistics(OutputTag<SourceTableStatistics> tag,
      SourceDataset.Digest datasetReg) {
    this.statsOutput = tag;
    this.datasetReg = datasetReg;
  }

  @Override
  public void processElement(SourceRecord.Raw sourceRecord, Context context,
      Collector<SourceRecord.Raw> out) throws Exception {
    SourceTableStatistics acc = stats.value();
    if (acc == null) {
      acc = new SourceTableStatistics();
      long timer =
          FlinkUtilities.getCurrentProcessingTime() + TimeUnit.MINUTES.toMillis(maxTimeInMin);
      nextTimer.update(timer);
      context.timerService().registerProcessingTimeTimer(timer);
      //Register an event timer into the far future to trigger when the stream ends
      context.timerService().registerEventTimeTimer(Long.MAX_VALUE);
    }
    ErrorCollector errors = acc.validate(sourceRecord, datasetReg);
    if (errors.isFatal()) {
      System.out.println("Stats Validation Error: " + errors);
      //TODO: Record is flawed, put it in sideoutput and issue warning
    } else {
      acc.add(sourceRecord, datasetReg);
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
  public void onTimer(long timestamp, OnTimerContext ctx, Collector<SourceRecord.Raw> out)
      throws Exception {
    SourceTableStatistics acc = stats.value();
    if (acc != null) {
      ctx.output(statsOutput, acc);
    }
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
