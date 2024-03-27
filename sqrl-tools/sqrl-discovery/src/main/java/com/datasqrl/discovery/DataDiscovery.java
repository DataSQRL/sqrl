/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.discovery;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.metadata.MetricStoreProvider;
import com.datasqrl.metadata.TableStatisticsStore;
import com.datasqrl.discovery.system.DataSystemDiscovery;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.formats.Format;
import com.datasqrl.metadata.stats.DefaultSchemaGenerator;
import com.datasqrl.metadata.stats.SourceTableStatistics;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.metadata.MetadataStoreProvider;
import com.datasqrl.schema.input.FlexibleTableSchema;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaFactory;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaHolder;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import lombok.Value;

@Value
public class DataDiscovery {

  DataDiscoveryConfig configuration;
  MonitoringJobFactory monitorFactory;
  MetadataStoreProvider metadataStore;
  NamePath basePath = NamePath.ROOT;


  private TableStatisticsStore openStore() throws IOException {
    return MetricStoreProvider.getStatsStore(metadataStore);
  }

  public MonitoringJobFactory.Job monitorTables(Collection<TableConfig> tables) {
    try (TableStatisticsStore store = openStore()) {
    } catch (IOException e) {
      configuration.getErrors().fatal("Could not open statistics store");
      return null;
    }

    return monitorFactory.create(tables, metadataStore);
  }

  public List<TableSource> discoverSchema(Collection<TableConfig> tables) {
    List<TableSource> resultTables = new ArrayList<>();
    try (TableStatisticsStore store = openStore()) {
      for (TableConfig table : tables) {
        if (table.getConnectorConfig().getFormat()
            .filter(DataDiscovery::isFlexibleFormat).isEmpty()) continue;
        SourceTableStatistics stats = store.getTableStatistics(NamePath.of(table.getName()));
        if (stats == null) {
          getConfiguration().getErrors().warn("Could not find data for table: %s", table.getName());
          continue;
        }
        DefaultSchemaGenerator generator = new DefaultSchemaGenerator(SchemaAdjustmentSettings.DEFAULT);
        Optional<FlexibleTableSchema> baseSchema = Optional.empty(); //TODO: allow users to configure base schemas
        FlexibleTableSchema schema;
        ErrorCollector subErrors = getConfiguration().getErrors().resolve(table.getName().getDisplay());
        if (baseSchema.isEmpty()) {
          schema = generator.mergeSchema(stats, table.getName(), subErrors);
        } else {
          schema = generator.mergeSchema(stats, baseSchema.get(), subErrors);
        }
        TableSource tblSource = table.initializeSource(basePath, new FlexibleTableSchemaHolder(schema));
        resultTables.add(tblSource);
      }
    } catch (IOException e) {
      getConfiguration().getErrors().fatal("Could not read statistics from store");

    }
    return resultTables;
  }

  public static boolean isFlexibleFormat(Format format) {
    return format.getSchemaType().filter(t -> t.equalsIgnoreCase(FlexibleTableSchemaFactory.SCHEMA_TYPE)).isPresent();
  }

  public List<TableSource> runFullDiscovery(DataSystemDiscovery systemDiscovery, String systemConfig) {
    Collection<TableConfig> inputTables = systemDiscovery.discoverTables(configuration, systemConfig);
    if (inputTables == null || inputTables.isEmpty()) {
      return List.of();
    }
    MonitoringJobFactory.Job monitorJob = monitorTables(inputTables);
    if (monitorJob == null) {
      return List.of();
    } else {
      monitorJob.execute("discovery");
    }
    List<TableSource> sourceTables = discoverSchema(inputTables);
    return sourceTables;
  }


}
