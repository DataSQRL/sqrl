package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.flink.EnvironmentProvider;
import ai.dataeng.sqml.flink.util.BufferedLatestSelector;
import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.source.*;
import com.google.common.base.Preconditions;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class DataSourceMonitor implements SourceTableListener {

    public static final int DEFAULT_PARALLELISM = 16;

    private final int defaultParallelism = DEFAULT_PARALLELISM;
    private final EnvironmentProvider envProvider;

    private Map<String, SourceDataset> sourceDatasets;

    public DataSourceMonitor(EnvironmentProvider envProvider) {
        this.envProvider = envProvider;
        this.sourceDatasets = new HashMap<>();
    }

    public void addDataset(SourceDataset dataset) {
        Preconditions.checkArgument(!sourceDatasets.containsKey(dataset.getName()),"Dataset with given name already exists: %s", dataset.getName());
        sourceDatasets.put(dataset.getName(),dataset);
        dataset.addSourceTableListener(this);

    }

    private KeySelector<SourceRecord,Integer> hashParallelizer = new KeySelector<SourceRecord, Integer>() {
        @Override
        public Integer getKey(SourceRecord sourceRecord) throws Exception {
            return sourceRecord.hashCode()%defaultParallelism;
        }
    };

    @Override
    public void registerSourceTable(SourceTable sourceTable) throws SourceTableListener.DuplicateException {
        StreamExecutionEnvironment flinkEnv = envProvider.get();
        DataStream<SourceRecord> data = sourceTable.getDataStream(flinkEnv);
        if (sourceTable.hasSchema()) {
            /* data = filter out all SourceRecords that don't match schema and put into side output for error reporting.
               schema is broadcast via Broadcst State (https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/fault-tolerance/state/#broadcast-state)
               so that we can support schema evolution in the future without having to restart the job.early
             */
            throw new NotImplementedException("Need to implement schema enforcement filter with side output to error sink");
        }
        final OutputTag<SourceTableStatistics.Accumulator> statsOutput = new OutputTag<>(getFlinkName(STATS_NAME_PREFIX, sourceTable)){};

        SingleOutputStreamOperator<SourceRecord> process = data.keyBy(hashParallelizer)
                                                                .process(new KeyedSourceRecordStatistics(statsOutput));

        process.addSink(new PrintSinkFunction<>());

        final int randomKey = FlinkUtilities.generateBalancedKey(defaultParallelism);

        process.getSideOutput(statsOutput).keyBy(FlinkUtilities.getSingleKeySelector(randomKey)).reduce(
                new ReduceFunction<SourceTableStatistics.Accumulator>() {
                    @Override
                    public SourceTableStatistics.Accumulator reduce(SourceTableStatistics.Accumulator acc, SourceTableStatistics.Accumulator add) throws Exception {
                        acc.merge(add);
                        return acc;
                    }
                }).map(new MapFunction<SourceTableStatistics.Accumulator, SourceTableStatistics>() {
                    @Override
                    public SourceTableStatistics map(SourceTableStatistics.Accumulator accumulator) throws Exception {
                        return accumulator.getLocalValue();
                    }
                }).keyBy(FlinkUtilities.getSingleKeySelector(randomKey)).process(new BufferedLatestSelector(getFlinkName(STATS_NAME_PREFIX, sourceTable),
                        500,
                        SourceTableStatistics.Accumulator.class), TypeInformation.of(SourceTableStatistics.Accumulator.class))
                .addSink(new PrintSinkFunction<>());
        try {
            flinkEnv.execute(getFlinkName(JOB_NAME_PREFIX, sourceTable));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final String JOB_NAME_PREFIX = "Monitor";
    public static final String STATS_NAME_PREFIX = "Statistics";

    private static String getFlinkName(String prefix, SourceTable table) {
        return prefix + "[" + table.getQualifiedName().toString() + "]";
    }

    public static class KeyedSourceRecordStatistics extends KeyedProcessFunction<Integer, SourceRecord, SourceRecord> {

        public static final String STATE_NAME_SUFFIX = "-state";

        private int maxRecords = 100000;
        private int maxTimeInSeconds = 3600;
        private OutputTag<SourceTableStatistics.Accumulator> statsOutput;

        private transient ValueState<SourceTableStatistics.Accumulator> stats;

        public KeyedSourceRecordStatistics(OutputTag<SourceTableStatistics.Accumulator> tag) {
            this.statsOutput = tag;
        }

        @Override
        public void processElement(SourceRecord sourceRecord, Context context, Collector<SourceRecord> out) throws Exception {
            SourceTableStatistics.Accumulator acc = stats.value();
            if (acc == null) {
                acc = new SourceTableStatistics.Accumulator();
                context.timerService().registerProcessingTimeTimer(3600 * 1000);
            }
            acc.add(sourceRecord);
            stats.update(acc);
            if (acc.getCount() >= maxRecords) {
                context.output(statsOutput,acc);
                stats.clear();
            }
            out.collect(sourceRecord);
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<SourceRecord> out) throws Exception {
            ctx.output(statsOutput,stats.value());
            stats.clear();
        }

        @Override
        public void open(Configuration config) {
            ValueStateDescriptor<SourceTableStatistics.Accumulator> descriptor =
                    new ValueStateDescriptor<>(statsOutput.getId()+STATE_NAME_SUFFIX, // the state name
                            TypeInformation.of(new TypeHint<SourceTableStatistics.Accumulator>() {}));
            stats = getRuntimeContext().getState(descriptor);
        }

    }

}
