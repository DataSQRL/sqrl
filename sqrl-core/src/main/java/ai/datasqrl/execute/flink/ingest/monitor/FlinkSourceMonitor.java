package ai.datasqrl.execute.flink.ingest.monitor;

import ai.datasqrl.config.provider.DatasetRegistryPersistenceProvider;
import ai.datasqrl.config.provider.JDBCConnectionProvider;
import ai.datasqrl.config.provider.MetadataStoreProvider;
import ai.datasqrl.config.provider.SerializerProvider;
import ai.datasqrl.execute.StreamEngine;
import ai.datasqrl.execute.flink.environment.FlinkStreamEngine;
import ai.datasqrl.execute.flink.environment.util.FlinkUtilities;
import ai.datasqrl.execute.flink.ingest.DataStreamProvider;
import ai.datasqrl.io.sources.SourceRecord;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.util.OutputTag;

public class FlinkSourceMonitor implements StreamEngine.SourceMonitor {

  public static final int DEFAULT_PARALLELISM = 16;
  public static final String STATS_NAME_PREFIX = "Stats";


  private final int defaultParallelism = DEFAULT_PARALLELISM;

  private final FlinkStreamEngine envProvider;
  private final JDBCConnectionProvider jdbc;
  private final DataStreamProvider streamProvider;
  private final MetadataStoreProvider metaProvider;
  private final SerializerProvider serializerProvider;
  private final DatasetRegistryPersistenceProvider registryProvider;

  public FlinkSourceMonitor(FlinkStreamEngine envProvider, JDBCConnectionProvider jdbc,
      MetadataStoreProvider metaProvider,
      SerializerProvider serializerProvider, DatasetRegistryPersistenceProvider registryProvider) {
    this.envProvider = envProvider;
    this.jdbc = jdbc;
    this.metaProvider = metaProvider;
    this.serializerProvider = serializerProvider;
    this.registryProvider = registryProvider;
    this.streamProvider = new DataStreamProvider();
  }

  @Override
  public FlinkStreamEngine.FlinkJob monitorTable(SourceTable sourceTable) {
    FlinkStreamEngine.Builder streamBuilder = envProvider.createStream();
    streamBuilder.setJobType(FlinkStreamEngine.JobType.MONITOR);

    DataStream<SourceRecord.Raw> data = null;
    data = streamProvider.getDataStream(sourceTable, streamBuilder);
    if (sourceTable.hasSchema()) {
            /* data = filter out all SourceRecords that don't match schema and put into side output for error reporting.
               schema is broadcast via Broadcst State (https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/datastream/fault-tolerance/state/#broadcast-state)
               so that we can support schema evolution in the future without having to restart the job.early
             */
      throw new NotImplementedException(
          "Need to implement schema enforcement filter with side output to error sink");
    }
    final OutputTag<SourceTableStatistics> statsOutput = new OutputTag<>(
        FlinkStreamEngine.getFlinkName(STATS_NAME_PREFIX, sourceTable.qualifiedName())) {
    };

    SingleOutputStreamOperator<SourceRecord.Raw> process = data.keyBy(
            FlinkUtilities.getHashPartitioner(defaultParallelism))
        .process(
            new KeyedSourceRecordStatistics(statsOutput, sourceTable.getDataset().getDigest()));
    process.addSink(new PrintSinkFunction<>()); //TODO: persist last 100 for querying

    //Process the gathered statistics in the side output
    final int randomKey = FlinkUtilities.generateBalancedKey(defaultParallelism);
    process.getSideOutput(statsOutput)
        .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
        .reduce(
            new ReduceFunction<SourceTableStatistics>() {
              @Override
              public SourceTableStatistics reduce(SourceTableStatistics acc,
                  SourceTableStatistics add) throws Exception {
                acc.merge(add);
                return acc;
              }
            })
        .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
//                .process(new BufferedLatestSelector(getFlinkName(STATS_NAME_PREFIX, sourceTable),
//                        500, SourceTableStatistics.Accumulator.class), TypeInformation.of(SourceTableStatistics.Accumulator.class))
        .addSink(new SaveTableStatistics(jdbc, metaProvider, serializerProvider, registryProvider,
            sourceTable.getDataset().getName(), sourceTable.getName()));
    return streamBuilder.build();
  }


}
