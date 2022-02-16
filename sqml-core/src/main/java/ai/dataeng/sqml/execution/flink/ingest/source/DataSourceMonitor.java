package ai.dataeng.sqml.execution.flink.ingest.source;

import ai.dataeng.sqml.config.provider.DatasetRegistryPersistenceProvider;
import ai.dataeng.sqml.config.provider.HierarchicalKeyValueStoreProvider;
import ai.dataeng.sqml.execution.flink.environment.FlinkStreamEngine;
import ai.dataeng.sqml.execution.flink.environment.util.FlinkUtilities;
import ai.dataeng.sqml.io.sources.SourceRecord;
import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import ai.dataeng.sqml.io.sources.dataset.SourceTableMonitor;
import ai.dataeng.sqml.execution.flink.ingest.stats.KeyedSourceRecordStatistics;
import ai.dataeng.sqml.execution.flink.ingest.stats.SourceTableStatistics;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

public class DataSourceMonitor implements SourceTableMonitor {

    public static final int DEFAULT_PARALLELISM = 16;

    private final int defaultParallelism = DEFAULT_PARALLELISM;

    private final FlinkStreamEngine envProvider;
    private final DataStreamProvider streamProvider;
    private final HierarchicalKeyValueStoreProvider hkvsProvider;
    private final DatasetRegistryPersistenceProvider persistenceProvider;

    public DataSourceMonitor(FlinkStreamEngine envProvider, HierarchicalKeyValueStoreProvider hkvsProvider,
                             DatasetRegistryPersistenceProvider persistenceProvider) {
        this.envProvider = envProvider;
        this.hkvsProvider = hkvsProvider;
        this.persistenceProvider = persistenceProvider;
        this.streamProvider = new DataStreamProvider();
    }

    @Override
    public void startTableMonitoring(SourceTable sourceTable) {
        StreamExecutionEnvironment flinkEnv = envProvider.create();

        DataStream<SourceRecord.Raw> data = null;
        data = streamProvider.getDataStream(sourceTable, flinkEnv);
        if (sourceTable.hasSchema()) {
            /* data = filter out all SourceRecords that don't match schema and put into side output for error reporting.
               schema is broadcast via Broadcst State (https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/fault-tolerance/state/#broadcast-state)
               so that we can support schema evolution in the future without having to restart the job.early
             */
            throw new NotImplementedException("Need to implement schema enforcement filter with side output to error sink");
        }
        final OutputTag<SourceTableStatistics> statsOutput = new OutputTag<>(getFlinkName(STATS_NAME_PREFIX, sourceTable)){};

        SingleOutputStreamOperator<SourceRecord.Raw> process = data.keyBy(FlinkUtilities.getHashPartitioner(defaultParallelism))
                                                                .process(new KeyedSourceRecordStatistics(statsOutput, sourceTable.getDataset().getDigest()));
        process.addSink(new PrintSinkFunction<>()); //TODO: persist last 100 for querying

        //Process the gathered statistics in the side output
        final int randomKey = FlinkUtilities.generateBalancedKey(defaultParallelism);
        process.getSideOutput(statsOutput)
                .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
                .reduce(
                    new ReduceFunction<SourceTableStatistics>() {
                        @Override
                        public SourceTableStatistics reduce(SourceTableStatistics acc, SourceTableStatistics add) throws Exception {
                            acc.merge(add);
                            return acc;
                        }
                })
                .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
//                .process(new BufferedLatestSelector(getFlinkName(STATS_NAME_PREFIX, sourceTable),
//                        500, SourceTableStatistics.Accumulator.class), TypeInformation.of(SourceTableStatistics.Accumulator.class))
                .addSink(new SaveTableStatistics(hkvsProvider,persistenceProvider,sourceTable.getDataset().getName(),sourceTable.getName()));
        try {
            flinkEnv.execute(getFlinkName(JOB_NAME_PREFIX, sourceTable));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static final String JOB_NAME_PREFIX = "Monitor";
    public static final String STATS_NAME_PREFIX = "Statistics";

    private static String getFlinkName(String prefix, SourceTable table) {
        return prefix + "[" + table.getDataset().getName() + "." + table.getName() + "]";
    }


}
