/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import static com.datasqrl.engine.stream.flink.FlinkStreamBuilder.STATS_NAME_PREFIX;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.StreamEngine.Builder;
import com.datasqrl.engine.stream.flink.FlinkStreamBuilder;
import com.datasqrl.engine.stream.flink.FlinkStreamEngine;
import com.datasqrl.engine.stream.flink.FlinkStreamHolder;
import com.datasqrl.engine.stream.flink.monitor.KeyedSourceRecordStatistics;
import com.datasqrl.engine.stream.flink.monitor.SaveTableStatistics;
import com.datasqrl.engine.stream.flink.util.FlinkUtilities;
import com.datasqrl.engine.stream.inmemory.InMemStreamEngine;
import com.datasqrl.engine.stream.inmemory.InMemStreamEngine.JobBuilder;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.SourceRecord.Raw;
import com.datasqrl.io.stats.TableStatisticsStoreProvider.Encapsulated;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.stats.DefaultSchemaGenerator;
import com.datasqrl.io.stats.SourceTableStatistics;
import com.datasqrl.io.stats.TableStatisticsStore;
import com.datasqrl.io.stats.TableStatisticsStoreProvider;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.StreamInputPreparer;
import com.datasqrl.io.util.StreamInputPreparerImpl;
import com.datasqrl.metadata.MetadataNamedPersistence;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.input.FlexibleDatasetSchema;
import com.datasqrl.schema.input.FlexibleDatasetSchema.TableField;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;

import com.google.common.base.Preconditions;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.util.OutputTag;

public class DataDiscovery {

  private final ErrorCollector errors;
  private final EngineSettings settings;
  private final StreamEngine streamEngine;
  private final TableStatisticsStoreProvider.Encapsulated statsStore;
  private final StreamInputPreparer streamPreparer;

  public DataDiscovery(ErrorCollector errors, EngineSettings settings) {
    this.errors = errors;
    this.settings = settings;
    streamEngine = settings.getStream();
    statsStore = new TableStatisticsStoreProvider.EncapsulatedImpl(
        settings.getMetadataStoreProvider(),
        new MetadataNamedPersistence.TableStatsProvider());
    streamPreparer = new StreamInputPreparerImpl();
  }

  public List<TableInput> discoverTables(DataSystemConfig discoveryConfig) {
    DataSystem dataSystem = discoveryConfig.initialize(errors);
    if (dataSystem == null) {
      return List.of();
    }

    NamePath path = NamePath.of(dataSystem.getName());
    List<TableInput> tables = dataSystem.getDatasource()
        .discoverSources(dataSystem.getConfig(), errors)
        .stream().map(tblConfig -> tblConfig.initializeInput(errors, path))
        .filter(tbl -> tbl != null && streamPreparer.isRawInput(tbl))
        .collect(Collectors.toList());
    return tables;
  }

  public StreamEngine.Job monitorTables(List<TableInput> tables) {
    try (TableStatisticsStore store = statsStore.openStore()) {
    } catch (IOException e) {
      errors.fatal("Could not open statistics store");
      return null;
    }
    StreamEngine.Builder streamBuilder = streamEngine.createJob();
    for (TableInput table : tables) {
      StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(table, streamBuilder,
          ErrorPrefix.INPUT_DATA.resolve(table.getName()));
      //todo: monitor
      stream = monitor(streamBuilder, stream, table, statsStore);
//      stream.printSink();
    }
    StreamEngine.Job job = streamBuilder.build();
    job.execute("monitoring[" + tables.size() + "]" + tables.hashCode());
    return job;
  }

  private StreamHolder<Raw> monitor(Builder streamBuilder, StreamHolder<Raw> stream,
      TableInput tableSource, Encapsulated statisticsStoreProvider) {
    if (streamBuilder instanceof InMemStreamEngine.JobBuilder) {
      InMemStreamEngine.JobBuilder builder = (InMemStreamEngine.JobBuilder) streamBuilder;
      return monitorInMem(builder, stream, tableSource, statisticsStoreProvider);
    } else if (streamBuilder instanceof FlinkStreamBuilder) {
      FlinkStreamBuilder builder = (FlinkStreamBuilder) streamBuilder;
      return monitorFlink(builder, stream, tableSource, statisticsStoreProvider);
    } else {
      throw new RuntimeException("Unknown engine type");
    }
  }

  private StreamHolder<Raw> monitorInMem(JobBuilder builder, StreamHolder<Raw> stream,
      TableInput tableSource, Encapsulated statisticsStoreProvider) {
    final SourceTableStatistics statistics = new SourceTableStatistics();
    final TableSource.Digest tableDigest = tableSource.getDigest();
    StreamHolder<SourceRecord.Raw> result = stream.mapWithError((r, c) -> {
      com.datasqrl.error.ErrorCollector errors = c.get();
      statistics.validate(r, tableDigest, errors);
      if (!errors.isFatal()) {
        statistics.add(r, tableDigest);
        return Optional.of(r);
      } else {
        return Optional.empty();
      }
    }, "stats", ErrorPrefix.INPUT_DATA.resolve(tableSource.getName()), SourceRecord.Raw.class);
    builder.getSideStreams().add(Stream.of(statistics).map(s -> {
      try (TableStatisticsStore store = statisticsStoreProvider.openStore()) {
        store.putTableStatistics(tableDigest.getPath(), s);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      return s;
    }));
    return result;
  }

  public StreamHolder<SourceRecord.Raw> monitorFlink(FlinkStreamBuilder builder,
      StreamHolder<Raw> stream,
      TableInput tableSource,
      Encapsulated statisticsStoreProvider) {
    Preconditions.checkArgument(stream instanceof FlinkStreamHolder);
    builder.setJobType(FlinkStreamEngine.JobType.MONITOR);

    DataStream<Raw> data = ((FlinkStreamHolder) stream).getStream();
    final OutputTag<SourceTableStatistics> statsOutput = new OutputTag<>(
        FlinkStreamEngine.getFlinkName(STATS_NAME_PREFIX, tableSource.qualifiedName())) {
    };

    SingleOutputStreamOperator<Raw> process = data.keyBy(
            FlinkUtilities.getHashPartitioner(builder.getDefaultParallelism()))
        .process(
            new KeyedSourceRecordStatistics(statsOutput, tableSource.getDigest()),
            TypeInformation.of(SourceRecord.Raw.class));
//    process.addSink(new PrintSinkFunction<>()); //TODO: persist last 100 for querying

    //Process the gathered statistics in the side output
    final int randomKey = FlinkUtilities.generateBalancedKey(builder.getDefaultParallelism());
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
        .addSink(new SaveTableStatistics(statisticsStoreProvider, tableSource.getDigest()));
    return new FlinkStreamHolder<>(builder, process);
  }

  public List<TableSource> discoverSchema(List<TableInput> tables) {
    return discoverSchema(tables, FlexibleDatasetSchema.EMPTY);
  }

  public List<TableSource> discoverSchema(List<TableInput> tables,
      FlexibleDatasetSchema baseSchema) {
    List<TableSource> resultTables = new ArrayList<>();
    try (TableStatisticsStore store = statsStore.openStore()) {
      for (TableInput table : tables) {
        SourceTableStatistics stats = store.getTableStatistics(table.getPath());
        if (stats == null) {
          //No data in table
          continue;
        }
        DefaultSchemaGenerator generator = new DefaultSchemaGenerator(SchemaAdjustmentSettings.DEFAULT);
        Optional<FlexibleDatasetSchema.TableField> tableField = baseSchema.getFieldByName(table.getName());
        FlexibleDatasetSchema.TableField schema;
        ErrorCollector subErrors = errors.resolve(table.getName());
        if (tableField.isEmpty()) {
          schema = generator.mergeSchema(stats, table.getName(), subErrors);
        } else {
          schema = generator.mergeSchema(stats, tableField.get(), subErrors);
        }
        TableSource tblSource = table.getConfiguration()
            .initializeSource(errors, table.getPath().parent(), schema);
        resultTables.add(tblSource);
      }
    } catch (IOException e) {
      errors.fatal("Could not read statistics from store");

    }
    return resultTables;
  }

  public List<TableSource> runFullDiscovery(DataSystemConfig discoveryConfig) {
    List<TableInput> inputTables = discoverTables(discoveryConfig);
    if (inputTables == null || inputTables.isEmpty()) {
      return List.of();
    }
    StreamEngine.Job monitorJob = monitorTables(inputTables);
    if (monitorJob == null) {
      return List.of();
    }
    //TODO: figure out how to wait on job completion
    List<TableSource> sourceTables = discoverSchema(inputTables);
    return sourceTables;

  }

  public static FlexibleDatasetSchema combineSchema(List<TableSource> tables) {
    return combineSchema(tables, FlexibleDatasetSchema.EMPTY);
  }

  public static FlexibleDatasetSchema combineSchema(List<TableSource> tables,
      FlexibleDatasetSchema baseSchema) {
    FlexibleDatasetSchema.Builder builder = new FlexibleDatasetSchema.Builder();
    builder.setDescription(baseSchema.getDescription());
    for (TableSource table : tables) {
      builder.add((TableField) table.getSchema().getSchema());
    }
    return builder.build();
  }


}
