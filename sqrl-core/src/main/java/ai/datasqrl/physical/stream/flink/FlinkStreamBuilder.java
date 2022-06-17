package ai.datasqrl.physical.stream.flink;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.function.Predicate;

import ai.datasqrl.config.provider.TableStatisticsStoreProvider;
import ai.datasqrl.physical.stream.StreamHolder;
import ai.datasqrl.physical.stream.flink.monitor.KeyedSourceRecordStatistics;
import ai.datasqrl.physical.stream.flink.monitor.SaveTableStatistics;
import ai.datasqrl.physical.stream.flink.schema.FlinkRowConstructor;
import ai.datasqrl.physical.stream.flink.schema.FlinkTableSchemaGenerator;
import ai.datasqrl.physical.stream.flink.schema.FlinkTypeInfoSchemaGenerator;
import ai.datasqrl.physical.stream.flink.util.FlinkUtilities;
import ai.datasqrl.io.formats.TextLineFormat;
import ai.datasqrl.io.impl.file.DirectorySourceImplementation;
import ai.datasqrl.io.impl.file.FilePath;
import ai.datasqrl.io.impl.kafka.KafkaSourceImplementation;
import ai.datasqrl.io.sources.*;
import ai.datasqrl.io.sources.dataset.SourceTable;
import ai.datasqrl.io.sources.stats.SourceTableStatistics;
import ai.datasqrl.io.sources.util.TimeAnnotatedRecord;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NameCanonicalizer;
import ai.datasqrl.schema.converters.SourceRecord2RowMapper;
import ai.datasqrl.schema.input.FlexibleDatasetSchema;
import ai.datasqrl.schema.input.FlexibleTableConverter;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.file.src.enumerate.NonSplittingRecursiveEnumerator;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.kafka.clients.consumer.ConsumerRecord;

@Getter
//TODO: Do output tags (errors, monitor) need a globally unique name or just local to the job?
public class FlinkStreamBuilder implements FlinkStreamEngine.Builder {

  public static final int DEFAULT_PARALLELISM = 16;
  public static final String STATS_NAME_PREFIX = "Stats";

  private final FlinkStreamEngine engine;
  private final StreamExecutionEnvironment environment;
  private final StreamTableEnvironment tableEnvironment;
  private final UUID uuid;
  private final int defaultParallelism = DEFAULT_PARALLELISM;
  private FlinkStreamEngine.JobType jobType;

  public static final String ERROR_TAG_PREFIX = "error";
  private Map<String, OutputTag<ProcessError>> errorTags = new HashMap<>();


  public FlinkStreamBuilder(FlinkStreamEngine engine, StreamExecutionEnvironment environment) {
    this.engine = engine;
    this.environment = environment;
    this.tableEnvironment = StreamTableEnvironment.create(environment);
    this.uuid = UUID.randomUUID();
  }

  @Override
  public void setJobType(@NonNull FlinkStreamEngine.JobType jobType) {
    this.jobType = jobType;
  }

  @Override
  public FlinkStreamEngine.FlinkJob build() {
    return engine.createStreamJob(environment, jobType);
  }

  @Override
  public OutputTag<ProcessError> getErrorTag(final String errorName) {
    OutputTag<ProcessError> errorTag = errorTags.get(errorName);
    if (errorTag==null) {
      errorTag = new OutputTag<>(
              FlinkStreamEngine.getFlinkName(ERROR_TAG_PREFIX, errorName)) {
      };
      errorTags.put(errorName, errorTag);
    }
    return errorTag;
  }

  @Override
  public StreamHolder<SourceRecord.Raw> monitor(StreamHolder<SourceRecord.Raw> stream,
                                                SourceTable sourceTable,
                                                TableStatisticsStoreProvider.Encapsulated statisticsStoreProvider) {
    Preconditions.checkArgument(stream instanceof FlinkStreamHolder && ((FlinkStreamHolder)stream).getBuilder().equals(this));
    setJobType(FlinkStreamEngine.JobType.MONITOR);

    DataStream<SourceRecord.Raw> data = ((FlinkStreamHolder)stream).getStream();
    final OutputTag<SourceTableStatistics> statsOutput = new OutputTag<>(
            FlinkStreamEngine.getFlinkName(STATS_NAME_PREFIX, sourceTable.qualifiedName())) {
    };

    SingleOutputStreamOperator<SourceRecord.Raw> process = data.keyBy(
                    FlinkUtilities.getHashPartitioner(defaultParallelism))
            .process(
                    new KeyedSourceRecordStatistics(statsOutput, sourceTable.getDataset().getDigest()), TypeInformation.of(SourceRecord.Raw.class));
//    process.addSink(new PrintSinkFunction<>()); //TODO: persist last 100 for querying

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
            .addSink(new SaveTableStatistics(statisticsStoreProvider,
                    sourceTable.getDataset().getName(), sourceTable.getName()));
    return new FlinkStreamHolder<>(this,process);
  }

  @Override
  public void addAsTable(StreamHolder<SourceRecord.Named> stream, FlexibleDatasetSchema.TableField schema, Name tableName) {
    Preconditions.checkArgument(stream instanceof FlinkStreamHolder && ((FlinkStreamHolder)stream).getBuilder().equals(this));
    FlinkStreamHolder<SourceRecord.Named> flinkStream = (FlinkStreamHolder)stream;
    FlexibleTableConverter converter = new FlexibleTableConverter(schema);

    TypeInformation typeInformation = FlinkTypeInfoSchemaGenerator.convert(converter);
    SourceRecord2RowMapper<Row,Row> mapper = new SourceRecord2RowMapper(schema, FlinkRowConstructor.INSTANCE);

    //TODO: error handling when mapping doesn't work?
    SingleOutputStreamOperator<Row> rows = flinkStream.getStream().map(r -> mapper.apply(r),typeInformation);
    Schema tableSchema = FlinkTableSchemaGenerator.convert(converter);
    Table table = tableEnvironment.fromDataStream(rows, tableSchema);
    tableEnvironment.createTemporaryView(tableName.getCanonical(), table);
  }

  public StreamHolder<TimeAnnotatedRecord<String>> fromTextSource(SourceTable table) {
    SourceTableConfiguration tblConfig = table.getConfiguration();
    Preconditions.checkArgument(tblConfig.getFormatParser() instanceof TextLineFormat.Parser, "This method only supports text sources");
    DataSource source = table.getDataset().getSource();
    DataSourceImplementation sourceImpl = source.getImplementation();
    DataSourceConfiguration sourceConfig = source.getConfig();
    String flinkSourceName = String.join("-", table.getDataset().getName().getDisplay(),
            tblConfig.getIdentifier(), "input");

    StreamExecutionEnvironment env = getEnvironment();
    DataStream<TimeAnnotatedRecord<String>> timedSource;
    if (sourceImpl instanceof DirectorySourceImplementation) {
      DirectorySourceImplementation filesource = (DirectorySourceImplementation) sourceImpl;

      Duration monitorDuration = null;
//            if (filesource.getConfiguration().isDiscoverFiles()) monitorDuration = Duration.ofSeconds(10);
      FileEnumeratorProvider fileEnumerator = new FileEnumeratorProvider(filesource,
              source.getCanonicalizer(), tblConfig);

      org.apache.flink.connector.file.src.FileSource.FileSourceBuilder<String> builder =
              org.apache.flink.connector.file.src.FileSource.forRecordStreamFormat(
                      new org.apache.flink.connector.file.src.reader.TextLineFormat(
                              sourceConfig.getCharset()), FilePath.toFlinkPath(filesource.getPath()));

      builder.setFileEnumerator(fileEnumerator);
      if (monitorDuration != null) {
        builder.monitorContinuously(Duration.ofSeconds(10));
      }

      //TODO: set watermarks
//              stream.assignTimestampsAndWatermarks(WatermarkStrategy.<SourceRecord>forMonotonousTimestamps().withTimestampAssigner((event, timestamp) -> event.getSourceTime().toEpochSecond()));

      DataStreamSource<String> textSource = env.fromSource(builder.build(),
              WatermarkStrategy.noWatermarks(), flinkSourceName);
      timedSource = textSource.map(new ToTimedRecord());

    } else if (sourceImpl instanceof KafkaSourceImplementation) {
      KafkaSourceImplementation kafkaSource = (KafkaSourceImplementation) sourceImpl;
      String topic = kafkaSource.getTopicPrefix() + tblConfig.getIdentifier();
      String groupId = flinkSourceName + "-" + getUuid();

      KafkaSourceBuilder<TimeAnnotatedRecord<String>> builder = KafkaSource.<TimeAnnotatedRecord<String>>builder()
              .setBootstrapServers(kafkaSource.getServersAsString())
              .setTopics(topic)
              .setStartingOffsets(OffsetsInitializer.earliest()) //TODO: work with commits
              .setGroupId(groupId);

      builder.setDeserializer(
              new KafkaTimeValueDeserializationSchemaWrapper<>(new SimpleStringSchema()));

      timedSource = env.fromSource(builder.build(),
              WatermarkStrategy.noWatermarks(), flinkSourceName);

    } else {
      throw new UnsupportedOperationException("Unrecognized source table type: " + table);
    }
    return new FlinkStreamHolder<>(this,timedSource);
  }

  private static class ToTimedRecord implements MapFunction<String,TimeAnnotatedRecord<String>> {

    @Override
    public TimeAnnotatedRecord<String> map(String s) throws Exception {
      return  new TimeAnnotatedRecord<>(s, null);
    }
  }

  @NoArgsConstructor
  @AllArgsConstructor
  private static class FileEnumeratorProvider implements
          org.apache.flink.connector.file.src.enumerate.FileEnumerator.Provider {

    DirectorySourceImplementation directorySource;
    NameCanonicalizer canonicalizer;
    SourceTableConfiguration table;

    @Override
    public org.apache.flink.connector.file.src.enumerate.FileEnumerator create() {
      return new NonSplittingRecursiveEnumerator(new FileNameMatcher());
    }

    private class FileNameMatcher implements Predicate<Path> {

      @Override
      public boolean test(Path path) {
        try {
          if (path.getFileSystem().getFileStatus(path).isDir()) {
            return true;
          }
        } catch (IOException e) {
          return false;
        }
        return directorySource.isTableFile(FilePath.fromFlinkPath(path), table, canonicalizer);
      }
    }
  }


  static class KafkaTimeValueDeserializationSchemaWrapper<T> implements
          KafkaRecordDeserializationSchema<TimeAnnotatedRecord<T>> {

    private static final long serialVersionUID = 1L;
    private final DeserializationSchema<T> deserializationSchema;

    KafkaTimeValueDeserializationSchemaWrapper(DeserializationSchema<T> deserializationSchema) {
      this.deserializationSchema = deserializationSchema;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
      deserializationSchema.open(context);
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> message,
                            Collector<TimeAnnotatedRecord<T>> out)
            throws IOException {
      T result = deserializationSchema.deserialize(message.value());
      if (result != null) {
        out.collect(new TimeAnnotatedRecord<>(result, Instant.ofEpochSecond(message.timestamp())));
      }
    }

    @Override
    public TypeInformation<TimeAnnotatedRecord<T>> getProducedType() {
      return (TypeInformation) TypeInformation.of(TimeAnnotatedRecord.class);
      //return deserializationSchema.getProducedType();
    }
  }

}
