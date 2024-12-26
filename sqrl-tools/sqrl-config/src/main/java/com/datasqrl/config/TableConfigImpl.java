/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.resolver.ResourceResolver;
import java.util.Optional;
import lombok.*;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;

@Value
public class TableConfigImpl implements TableConfig {

  public static final String FLINK_CONNECTOR_KEY = "flink";

  public static final String METADATA_KEY = "metadata";
  public static final String TABLE_KEY = "table";
  public static String METADATA_COLUMN_TYPE_KEY = "type";
  public static String METADATA_COLUMN_ATTRIBUTE_KEY = "attribute";
  public static String METADATA_VIRTUAL_ATTRIBUTE_KEY = "virtual";
  @NonNull Name name;
  @NonNull SqrlConfig config;

  public static final String FORMAT_NAME_KEY = "name";

  public static TableConfigBuilderImpl builder(String name) {
    return builder(Name.system(name));
  }

  public static TableConfigBuilderImpl builder(Name name) {
    return new TableConfigBuilderImpl(name, SqrlConfig.createCurrentVersion());
  }

  public void toFile(Path file, boolean pretty) {
    config.toFile(file, pretty);
  }

  public ErrorCollector getErrors() {
    return config.getErrorCollector();
  }

  @Override
  public ConnectorConfigImpl getConnectorConfig() {
    //Right now, we have hard-coded flink as the connector engine
    return new ConnectorConfigImpl(config.getSubConfig(FLINK_CONNECTOR_KEY) /*, new FlinkConnectorFactory()*/);
  }

  @Override
  public MetadataConfigImpl getMetadataConfig() {
    return new MetadataConfigImpl(config.getSubConfig(METADATA_KEY));
  }

  @Override
  public TableTableConfigImpl getBase() {
    return new TableTableConfigImpl(config.getSubConfig(TABLE_KEY));
  }

  @Override
  public List<String> getPrimaryKeyConstraint() {
    List<String> primaryKey = getBase().getPrimaryKey().get();

    // Some sinks disallow primary key (kafka) for sinks. Generally we should pass through when specified.
    // When creating the log engine, we should pass in if the connector needs pks. However, this is difficult
    // right now so we'll add a hack to check for this specific case.
    Optional<String> connectorName = getConnectorConfig().getConnectorName();
    if (connectorName.isPresent() && connectorName.get().equalsIgnoreCase("kafka")) {
      return List.of();
    }

    if (!primaryKey.isEmpty()) {
      return primaryKey;
    }

    return List.of();
  }
//  public TableSource initializeSource(NamePath basePath, TableSchema schema) {
//    getErrors().checkFatal(getBase().getType().isSource(), "Table is not a source: %s", name);
//    Name tableName = getName();
//    return new TableSource(this, basePath.concat(tableName), tableName, schema);
//  }
//
//  public TableSink initializeSink(NamePath basePath, Optional<TableSchema> schema) {
//    getErrors().checkFatal(getBase().getType().isSink(), "Table is not a sink: %s", name);
//    Name tableName = getName();
//    return new TableSinkImpl(this, basePath.concat(tableName), tableName, schema);
//  }

  public TableConfigBuilderImpl toBuilder() {
    return new TableConfigBuilderImpl(this.getName(), SqrlConfig.create(config)).copyFrom(this);
  }
}
