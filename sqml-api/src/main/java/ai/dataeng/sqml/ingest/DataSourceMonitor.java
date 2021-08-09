package ai.dataeng.sqml.ingest;

import ai.dataeng.sqml.db.keyvalue.HierarchyKeyValueStore;
import ai.dataeng.sqml.flink.EnvironmentProvider;
import ai.dataeng.sqml.flink.SaveToKeyValueStoreSink;
import ai.dataeng.sqml.flink.util.BufferedLatestSelector;
import ai.dataeng.sqml.flink.util.FlinkUtilities;
import ai.dataeng.sqml.source.*;
import com.google.common.base.Preconditions;
import lombok.NonNull;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

import java.util.HashMap;
import java.util.Map;

public class DataSourceMonitor implements SourceTableListener {

    public static final int DEFAULT_PARALLELISM = 16;
    public static final String STATS_KEY = "stats";
    public static final String DATA_KEY = "data";

    private final int defaultParallelism = DEFAULT_PARALLELISM;
    private final EnvironmentProvider envProvider;

    private Map<String, SourceDataset> sourceDatasets;
    private HierarchyKeyValueStore.Factory storeFactory;
    private HierarchyKeyValueStore store;


    public DataSourceMonitor(EnvironmentProvider envProvider, HierarchyKeyValueStore.Factory storeFactory) {
        this.envProvider = envProvider;
        this.sourceDatasets = new HashMap<>();
        this.storeFactory = storeFactory;
        this.store = storeFactory.open();
    }

    public void addDataset(@NonNull SourceDataset dataset) {
        Preconditions.checkArgument(!sourceDatasets.containsKey(dataset.getName()),"Dataset with given name already exists: %s", dataset.getName());
        sourceDatasets.put(dataset.getName(),dataset);
        dataset.addSourceTableListener(this);

    }

    public SourceDataset getDataset(@NonNull String name) {
        return sourceDatasets.get(name);
    }

    public SourceTableStatistics getTableStatistics(@NonNull SourceTable table) {
        SourceTableStatistics.Accumulator acc = store.get(SourceTableStatistics.Accumulator.class,
                table.getQualifiedName().toString(), STATS_KEY);
        if (acc==null) return null;
        return acc.getLocalValue();
    }

    public SourceTableStatistics getTableStatistics(@NonNull SourceTableQualifiedName tableName) {
        return getTableStatistics(getTable(tableName));
    }

    public SourceTable getTable(@NonNull SourceTableQualifiedName tableName) {
        SourceDataset dataset = getDataset(tableName.getDataset());
        Preconditions.checkArgument(dataset!=null,"Dataset not found: %s", tableName.getDataset());
        SourceTable table = dataset.getTable(tableName.getTable());
        Preconditions.checkArgument(table!=null,"Table [%s] not found in dataset [%s]", tableName.getTable(), tableName.getDataset());
        return table;
    }

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

        SingleOutputStreamOperator<SourceRecord> process = data.keyBy(FlinkUtilities.getHashPartitioner(defaultParallelism))
                                                                .process(new KeyedSourceRecordStatistics(statsOutput));
        process.addSink(new PrintSinkFunction<>()); //TODO: persist last 100 for querying

        //Process the gathered statistics in the side output
        final int randomKey = FlinkUtilities.generateBalancedKey(defaultParallelism);
        process.getSideOutput(statsOutput)
                .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
                .reduce(
                    new ReduceFunction<SourceTableStatistics.Accumulator>() {
                        @Override
                        public SourceTableStatistics.Accumulator reduce(SourceTableStatistics.Accumulator acc, SourceTableStatistics.Accumulator add) throws Exception {
                            acc.merge(add);
                            return acc;
                        }
                })
                .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
                .process(new BufferedLatestSelector(getFlinkName(STATS_NAME_PREFIX, sourceTable),
                        500, SourceTableStatistics.Accumulator.class), TypeInformation.of(SourceTableStatistics.Accumulator.class))
                .addSink(new SaveToKeyValueStoreSink(storeFactory,SourceTableStatistics.Accumulator.class, sourceTable.getQualifiedName().toString(), STATS_KEY));
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


}
