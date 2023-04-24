/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.config.SerializedSqrlConfig;
import com.datasqrl.config.SqrlConfig;
import com.datasqrl.config.SqrlConfigCommons;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.DataSystemDiscoveryFactory;
import com.datasqrl.io.DataSystemImplementationFactory;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.schema.input.SchemaValidator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import java.io.Serializable;
import java.net.URI;
import java.nio.file.Path;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;

import java.util.Optional;

@Value
public class TableConfig {

  public static final String CONNECTOR_KEY = "connector";
  public static final String FORMAT_KEY = "format";

  @NonNull Name name;
  @NonNull SqrlConfig config;
  @NonNull BaseTableConfig base;

  public TableConfig(@NonNull Name name, @NonNull SqrlConfig config) {
    this.name = name;
    this.config = config;
    this.base = config.allAs(BaseTableConfig.class).get();
  }

  public static TableConfig load(@NonNull URI uri, @NonNull Name name, @NonNull ErrorCollector errors) {
    SqrlConfig config = SqrlConfigCommons.fromURL(errors, ResourceResolver.toURL(uri));
    return new TableConfig(name, config);
  }

  public static TableConfig load(@NonNull Path path, @NonNull Name name, @NonNull ErrorCollector errors) {
    SqrlConfig config = SqrlConfigCommons.fromFiles(errors, path);
    return new TableConfig(name, config);
  }

  public void toFile(Path file) {
    config.toFile(file, true);
  }

  public ErrorCollector getErrors() {
    return config.getErrorCollector();
  }

  public SqrlConfig getConnectorConfig() {
    return config.getSubConfig(CONNECTOR_KEY);
  }

  public DataSystemConnector getConnector() {
    return DataSystemConnectorFactory.fromConfig(this);
  }

  public String getConnectorName() {
    return getConnectorConfig().asString(DataSystemImplementationFactory.SYSTEM_NAME_KEY).get();
  }

  /**
   * TODO: make private and return Format object for getFormat that combines FormatFactory with SqrlConfig
   * @return
   */
  public SqrlConfig getFormatConfig() {
    return config.getSubConfig(FORMAT_KEY);
  }

  public boolean hasFormat() {
    return getFormatConfig().containsKey(FormatFactory.FORMAT_NAME_KEY);
  }

  public FormatFactory getFormat() {
    SqrlConfig formatConfig = getFormatConfig();
    FormatFactory factory = FormatFactory.fromConfig(formatConfig);
    return factory;
  }

  /**
   * TODO: read from config
   *
   * @return
   */
  @JsonIgnore
  public SchemaAdjustmentSettings getSchemaAdjustmentSettings() {
    return SchemaAdjustmentSettings.DEFAULT;
  }


  private void validateTable() {
    getErrors().checkFatal(!Strings.isNullOrEmpty(base.getIdentifier()), "Need to specify a table identifier");
    getErrors().checkFatal(hasFormat(), "Need to define a table format");
  }

  public TableSource initializeSource(NamePath basePath, TableSchema schema) {
    validateTable();
    getErrors().checkFatal(base.getType().isSource(), "Table is not a source: %s", name);
    DataSystemConnector connector = getConnector();
    Name tableName = getName();
    SchemaValidator validator = schema.getValidator(this.getSchemaAdjustmentSettings(), connector.hasSourceTimestamp());
    return new TableSource(connector, this, basePath.concat(tableName), tableName, schema, validator);
  }

  public TableInput initializeInput(NamePath basePath) {
    validateTable();
    getErrors().checkFatal(base.getType().isSource(), "Table is not a source: %s", name);
    DataSystemConnector connector = getConnector();
    Name tableName = getName();
    return new TableInput(connector, this, basePath.concat(tableName), tableName);
  }

  public TableSink initializeSink(ErrorCollector errors, NamePath basePath,
      Optional<TableSchema> schema) {
    validateTable();
    getErrors().checkFatal(base.getType().isSink(), "Table is not a sink: %s", name);
    DataSystemConnector connector = getConnector();
    Name tableName = getName();
    return new TableSink(connector,this, basePath.concat(tableName), tableName, schema);
  }

  public DataSystemDiscovery initializeDiscovery() {
    DataSystemDiscoveryFactory factory = DataSystemImplementationFactory.fromConfig(DataSystemDiscoveryFactory.class, this);
    DataSystemDiscovery discovery = factory.initialize(this);
    getErrors().checkFatal(!discovery.requiresFormat(base.getType()) || hasFormat(),
        "Data Discovery [%s] requires a format", discovery);
    return discovery;
  }

  public static Builder builder(String name) {
    return builder(Name.system(name));
  }

  public static Builder builder(Name name) {
    return new Builder(name, SqrlConfig.create());
  }

  public Builder toBuilder() {
    return new Builder(this.getName(), SqrlConfig.create()).copyFrom(this);
  }

  @Value
  public static class Builder {

    @NonNull Name name;
    @NonNull SqrlConfig config;

    public Builder copyFrom(TableConfig otherTable) {
      config.copy(otherTable.config);
      return this;
    }

    public Builder base(BaseTableConfig baseConfig) {
      config.setProperties(baseConfig);
      return this;
    }

    public Builder schema(String schema) {
      config.setProperty(BaseTableConfig.SCHEMA_KEY, schema);
      return this;
    }

    public SqrlConfig getConnectorConfig() {
      return config.getSubConfig(TableConfig.CONNECTOR_KEY);
    }

    public SqrlConfig getFormatConfig() {
      return config.getSubConfig(TableConfig.FORMAT_KEY);
    }

    public TableConfig build() {
      return new TableConfig(name,config);
    }

  }

  public Serialized serialize() {
    return new Serialized(name,config.serialize());
  }

  @AllArgsConstructor
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class Serialized {

    Name name;
    SerializedSqrlConfig config;

    public TableConfig deserialize(ErrorCollector errors) {
      return new TableConfig(name, config.deserialize(errors));
    }

  }

}
