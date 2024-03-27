/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.connector.ConnectorConfig;
import com.datasqrl.module.resolver.ResourceResolver;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Preconditions;
import lombok.*;

import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

@Value
public class TableConfig {

  public static final String CONNECTOR_KEY = "flink";
  public static final String CONNECTOR_NAME_KEY = "connector";

  public static final String METADATA_KEY = "metadata";
  public static final String TABLE_KEY = "table";
  public static String METADATA_COLUMN_TYPE_KEY = "type";
  public static String METADATA_COLUMN_ATTRIBUTE_KEY = "attribute";
  @NonNull Name name;
  @NonNull SqrlConfig config;

  public static final String FORMAT_NAME_KEY = "name";

  public static TableConfig load(@NonNull URI uri, @NonNull Name name,
      @NonNull ErrorCollector errors) {
    SqrlConfig config = SqrlConfigCommons.fromURL(errors, ResourceResolver.toURL(uri));
    return new TableConfig(name, config);
  }

  public static TableConfig load(@NonNull Path path, @NonNull Name name,
      @NonNull ErrorCollector errors) {
    SqrlConfig config = SqrlConfigCommons.fromFiles(errors, path);
    return new TableConfig(name, config);
  }

  public static Builder builder(String name) {
    return builder(Name.system(name));
  }

  public static Builder builder(Name name) {
    return new Builder(name, SqrlConfig.createCurrentVersion());
  }

  public void toFile(Path file) {
    config.toFile(file, true);
  }

  public ErrorCollector getErrors() {
    return config.getErrorCollector();
  }

  public ConnectorConfig getConnectorConfig() {
    //Right now, we have hard-coded flink as the connector engine
    return new ConnectorConfig(config.getSubConfig(CONNECTOR_KEY), new FlinkConnectorFactory());
  }

  public SqrlConfig getMetadataConfig() {
    return config.getSubConfig(METADATA_KEY);
  }

  public Base getBaseTableConfig() {
    return new Base(config.getSubConfig(TABLE_KEY));
  }

  public Base getBase() {
    return getBaseTableConfig();
  }

  public List<String> getPrimaryKeyConstraint() {
    List<String> primaryKey = getBase().getPrimaryKey().get();

    // Some sinks disallow primary key (kafka) for sinks. Generally we should pass through when specified.
    // When creating the log engine, we should pass in if the connector needs pks. However, this is difficult
    // right now so we'll add a hack to check for this specific case.
    String connectorName = getConnectorConfig().getConfig()
        .asString(FlinkConnectorFactory.CONNECTOR_KEY).get();
    if (connectorName.equalsIgnoreCase("kafka")) {
      return List.of();
    }

    if (!primaryKey.isEmpty()) {
      return primaryKey;
    }

    return List.of();
  }

  public TableSource initializeSource(NamePath basePath, TableSchema schema) {
    getErrors().checkFatal(getBase().getType().isSource(), "Table is not a source: %s", name);
    Name tableName = getName();
    return new TableSource(this, basePath.concat(tableName), tableName, schema);
  }

  public TableSink initializeSink(NamePath basePath, Optional<TableSchema> schema) {
    getErrors().checkFatal(getBase().getType().isSink(), "Table is not a sink: %s", name);
    Name tableName = getName();
    return new TableSinkImpl(this, basePath.concat(tableName), tableName, schema);
  }

  public Builder toBuilder() {
    return new Builder(this.getName(), SqrlConfig.create(config)).copyFrom(this);
  }


  @Value
  public static class Base {

    public static final String TYPE_KEY = "type";
    public static final String TIMESTAMP_COL_KEY = "timestamp";
    public static final String WATERMARK_KEY = "watermark-millis";
    public static String PRIMARYKEY_KEY = "primary-key";
    public static String PARTITIONKEY_KEY = "partition-key";
    SqrlConfig baseConfig;

    public ExternalDataType getType() {
      return SqrlConfig.getEnum(baseConfig.asString(TYPE_KEY), ExternalDataType.class,
          Optional.of(ExternalDataType.source_and_sink));
    }

    public long getWatermarkMillis() {
      return baseConfig.asLong(WATERMARK_KEY).withDefault(-1L).get();
    }

    public SqrlConfig.Value<String> getTimestampColumn() {
      return baseConfig.asString(TIMESTAMP_COL_KEY);
    }

    public SqrlConfig.Value<List<String>> getPrimaryKey() {
      return baseConfig.asList(PRIMARYKEY_KEY, String.class);
    }

    public SqrlConfig getConfig() {
      return baseConfig;
    }

    public SqrlConfig.Value<List<String>> getPartitionKeys() {
      return baseConfig.asList(PARTITIONKEY_KEY, String.class);
    }
  }

  @Value
  public static class Builder {

    @NonNull Name name;
    @NonNull SqrlConfig config;

    public Builder copyFrom(TableConfig otherTable) {
      config.copy(otherTable.config);
      return this;
    }

    public SqrlConfig getBaseConfig() {
      return config.getSubConfig(TableConfig.TABLE_KEY);
    }

    public Builder setType(ExternalDataType type) {
      getBaseConfig().setProperty(Base.TYPE_KEY, type.name().toLowerCase());
      return this;
    }

    public Builder setWatermark(long milliseconds) {
      Preconditions.checkArgument(milliseconds >= 0, "Invalid watermark milliseconds: %s",
          milliseconds);
      getBaseConfig().setProperty(Base.WATERMARK_KEY, milliseconds);
      SqrlConfig config = getBaseConfig();
      return this;
    }

    public Builder setTimestampColumn(@NonNull String columnName) {
      getBaseConfig().setProperty(Base.TIMESTAMP_COL_KEY, columnName);
      return this;
    }

    public Builder setPrimaryKey(@NonNull String[] primaryKey) {
      getBaseConfig().setProperty(Base.PRIMARYKEY_KEY, primaryKey);
      return this;
    }

    public Builder setMetadata(@NonNull String columnName, String type, String attribute) {
      SqrlConfig colConfig = config.getSubConfig(METADATA_KEY).getSubConfig(columnName);
      colConfig.setProperty(METADATA_COLUMN_TYPE_KEY, type);
      colConfig.setProperty(METADATA_COLUMN_ATTRIBUTE_KEY, attribute);
      return this;
    }

    public Builder setMetadataFunction(@NonNull String columnName, String function) {
      SqrlConfig colConfig = config.getSubConfig(METADATA_KEY).getSubConfig(columnName);
      colConfig.setProperty(METADATA_COLUMN_ATTRIBUTE_KEY, function);
      return this;
    }

    public Builder addUuid(String columnName) {
      return setMetadataFunction(columnName, "secure.Uuid()");
    }

    public Builder addIngestTime(String columnName) {
      return setMetadataFunction(columnName, "proctime()");
    }

    @Deprecated
    public SqrlConfig getConnectorConfig() {
      return config.getSubConfig(TableConfig.CONNECTOR_KEY);
    }

    public Builder copyConnectorConfig(ConnectorConfig connectorConfig) {
      config.getSubConfig(TableConfig.CONNECTOR_KEY).copy(connectorConfig.getConfig());
      return this;
    }

    public SqrlConfig getConfig() {
      return config;
    }

    public TableConfig build() {
      return new TableConfig(name, config);
    }

  }

}
