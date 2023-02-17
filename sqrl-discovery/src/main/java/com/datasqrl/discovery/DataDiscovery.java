/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.config.EngineSettings;
import com.datasqrl.discovery.store.MetricStoreProvider;
import com.datasqrl.discovery.store.TableStatisticsStore;
import com.datasqrl.engine.EngineCapability;
import com.datasqrl.engine.stream.StreamEngine;
import com.datasqrl.engine.stream.StreamHolder;
import com.datasqrl.engine.stream.monitor.DataMonitor;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.error.ErrorPrefix;
import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.SourceRecord;
import com.datasqrl.io.stats.DefaultSchemaGenerator;
import com.datasqrl.io.stats.SourceTableStatistics;
import com.datasqrl.io.tables.TableInput;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.io.util.StreamInputPreparer;
import com.datasqrl.io.util.StreamInputPreparerImpl;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.google.common.base.Preconditions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class DataDiscovery {

  private final ErrorCollector errors;
  private final EngineSettings settings;
  private final StreamEngine streamEngine;
  private final MetadataStoreProvider metadataStoreProvider;
  private final StreamInputPreparer streamPreparer;

  public DataDiscovery(ErrorCollector errors, EngineSettings settings) {
    this.errors = errors;
    this.settings = settings;
    streamEngine = settings.getStream();
    Preconditions.checkArgument(streamEngine.supports(EngineCapability.DATA_MONITORING));
    this.metadataStoreProvider = settings.getMetadataStoreProvider();
    streamPreparer = new StreamInputPreparerImpl();
  }

  private TableStatisticsStore openStore() throws IOException {
    return MetricStoreProvider.getStatsStore(metadataStoreProvider);
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

  public DataMonitor.Job monitorTables(List<TableInput> tables) {
    try (TableStatisticsStore store = openStore()) {
    } catch (IOException e) {
      errors.fatal("Could not open statistics store");
      return null;
    }

    DataMonitor dataMonitor = streamEngine.createDataMonitor();
    for (TableInput table : tables) {
      StreamHolder<SourceRecord.Raw> stream = streamPreparer.getRawInput(table, dataMonitor,
          ErrorPrefix.INPUT_DATA.resolve(table.getName()));
      StreamHolder<SourceTableStatistics> stats = stream.mapWithError(new ComputeMetrics(table.getDigest()),
          ErrorPrefix.INPUT_DATA.resolve(table.getName()), SourceTableStatistics.class);
      dataMonitor.monitorTable(table, stats, new MetricStoreProvider(metadataStoreProvider,
          table.getDigest().getPath()));
    }
    DataMonitor.Job job = dataMonitor.build();
    return job;
  }

  public List<TableSource> discoverSchema(List<TableInput> tables) {
    List<TableSource> resultTables = new ArrayList<>();
    try (TableStatisticsStore store = openStore()) {
      for (TableInput table : tables) {
        SourceTableStatistics stats = store.getTableStatistics(table.getPath());
        if (stats == null) {
          //No data in table
          continue;
        }
        DefaultSchemaGenerator generator = new DefaultSchemaGenerator(SchemaAdjustmentSettings.DEFAULT);
        Optional<FlexibleTableSchema> baseSchema = Optional.empty(); //TODO: allow users to configure base schemas
        FlexibleTableSchema schema;
        ErrorCollector subErrors = errors.resolve(table.getName());
        if (baseSchema.isEmpty()) {
          schema = generator.mergeSchema(stats, table.getName(), subErrors);
        } else {
          schema = generator.mergeSchema(stats, baseSchema.get(), subErrors);
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
    DataMonitor.Job monitorJob = monitorTables(inputTables);
    if (monitorJob == null) {
      return List.of();
    } else {
      monitorJob.execute("discovery");
    }
    List<TableSource> sourceTables = discoverSchema(inputTables);
    return sourceTables;

  }


}
