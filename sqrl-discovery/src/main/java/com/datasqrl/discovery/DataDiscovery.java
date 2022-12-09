/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.stats.SchemaGenerator;
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
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

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
      stream = streamBuilder.monitor(stream, table, statsStore);
      stream.printSink();
    }
    StreamEngine.Job job = streamBuilder.build();
    job.execute("monitoring[" + tables.size() + "]" + tables.hashCode());
    return job;
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
        SchemaGenerator generator = new SchemaGenerator(SchemaAdjustmentSettings.DEFAULT);
        FlexibleDatasetSchema.TableField tableField = baseSchema.getFieldByName(table.getName());
        FlexibleDatasetSchema.TableField schema;
        ErrorCollector subErrors = errors.resolve(table.getName());
        if (tableField == null) {
          schema = generator.mergeSchema(stats, table.getName(), subErrors);
        } else {
          schema = generator.mergeSchema(stats, tableField, subErrors);
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
      builder.add(table.getSchema().getSchema());
    }
    return builder.build();
  }


}
