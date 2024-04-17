package com.datasqrl.discovery.flink;

import com.datasqrl.FunctionWithError;
import com.datasqrl.InputError;
import com.datasqrl.MapWithErrorProcess;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.TableConfig;
import com.datasqrl.discovery.ComputeMetrics;
import com.datasqrl.discovery.MonitoringJobFactory;
import com.datasqrl.discovery.SourceRecord;
import com.datasqrl.discovery.process.ParseJson;
import com.datasqrl.engine.stream.flink.AbstractFlinkStreamEngine;
import com.datasqrl.error.ErrorLocation;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.metadata.MetricStoreProvider;
import com.datasqrl.metadata.stats.SourceTableStatistics;
import java.util.Collection;
import java.util.Map;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.OutputTag;

@AllArgsConstructor
@Getter
public class FlinkMonitoringJobFactory implements MonitoringJobFactory {

  public static final int DEFAULT_PARALLELISM = 2;

  private final int defaultParallelism = DEFAULT_PARALLELISM;

  AbstractFlinkStreamEngine flinkEngine;
  ConnectorFactoryFactory connectorFactoryFactory;

  @Override
  public Job create(Collection<TableConfig> tables, MetadataStoreProvider storeProvider) {
    StreamExecutionEnvironment environment = new ExecutionEnvironmentFactory(getFlinkConfiguration())
        .createEnvironment();
    StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
    FlinkErrorHandler errorHandler = new FlinkErrorHandler();

    for (TableConfig table: tables) {
      ErrorLocation errorLocation = ErrorPrefix.INPUT_DATA.resolve(table.getName().getDisplay());
      DataStream<String> textStream = createTextStream(table, tableEnvironment);
      DataStream<SourceRecord.Raw> parsedStream = mapWithError(textStream, new ParseJson(), errorLocation, SourceRecord.Raw.class, errorHandler);

      DataStream<SourceTableStatistics> metricsStream = mapWithError(parsedStream, new ComputeMetrics(),
          errorLocation, SourceTableStatistics.class, errorHandler);
      //Aggregate the gathered statistics
      final int randomKey = FlinkUtilities.generateBalancedKey(getDefaultParallelism());
      metricsStream
          .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
          .reduce(
              new ReduceFunction<SourceTableStatistics>() {
                @Override
                public SourceTableStatistics reduce(SourceTableStatistics base,
                    SourceTableStatistics add) throws Exception {
                  base.merge(add);
                  return base;
                }
              })
          .keyBy(FlinkUtilities.getSingleKeySelector(randomKey))
          //TODO: add time window to buffer before writing to database for efficiency
          .addSink(new SaveMetricsSink<SourceTableStatistics>(new MetricStoreProvider(storeProvider, NamePath.of(table.getName()))));
    }
    errorHandler.getErrorStream().ifPresent(errStream -> errStream.print());
    return new FlinkJob(environment);
  }

  private DataStream<String> createTextStream(TableConfig table, StreamTableEnvironment tableEnv) {
//    SqrlConfig connectorConfig = table.getConnectorConfig().getConfig();
//    String connector = connectorConfig.asString(FlinkConnectorFactory.CONNECTOR_KEY).get();
//
//    TableDescriptor.Builder descriptorBuilder = TableDescriptor.forConnector(connector);
//    connectorConfig.getKeys().forEach(key -> descriptorBuilder.option(key, connectorConfig.asString(key).get()));
//    //Overwrite format
//    descriptorBuilder.format(FormatDescriptor.forFormat("raw").build());
//    descriptorBuilder.schema(Schema.newBuilder()
//        .column("data", DataTypes.STRING()).build());
//
//    tableEnv.createTable(table.getName().getDisplay(), descriptorBuilder.build());
//
//    DataStream<Row> rawStream = tableEnv.toDataStream(tableEnv.from(table.getName().getDisplay()));
//
//    DataStream<String> splitStream = rawStream.flatMap(new FlatMapFunction<Row, String>() {
//      @Override
//      public void flatMap(Row value, Collector<String> out) throws Exception {
//        for (String line : value.getField(0).toString().split("\n")) {
//          out.collect(line);
//        }
//      }
//    });
//    return splitStream;
    throw new RuntimeException();
  }

  public static <T, R> DataStream<R> mapWithError(DataStream<T> stream, FunctionWithError<T, R> function,
      ErrorLocation errorLocation, Class<R> clazz, FlinkErrorHandler errorHandler) {
    final OutputTag<InputError> errorTag = errorHandler.getTag();
    SingleOutputStreamOperator<R> result = stream.process(
        new MapWithErrorProcess<>(errorTag, function, errorLocation), TypeInformation.of(clazz));
    errorHandler.registerErrorStream(result.getSideOutput(errorTag)); //..addSink(new PrintSinkFunction<>());
    return result;
  }


  public Map<String,String> getFlinkConfiguration() {
    throw new RuntimeException();
//    return getFlinkConfiguration(flinkEngine.getConfig());
//  }
//
//  public static Map<String,String> getFlinkConfiguration(@NonNull SqrlConfig config) {
//    return SqrlConfigUtil.toStringMap(config, EngineFactory.getReservedKeys());
  }

}
