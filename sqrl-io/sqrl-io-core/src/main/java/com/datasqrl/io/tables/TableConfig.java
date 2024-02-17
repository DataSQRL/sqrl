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
import com.datasqrl.io.DataSystemConnectorFactory;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.DataSystemDiscoveryFactory;
import com.datasqrl.io.DataSystemImplementationFactory;
import com.datasqrl.io.formats.FormatFactory;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.google.common.base.Strings;
import java.net.URI;
import java.nio.file.Path;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.Value;

@Value
public class TableConfig {

  public static final String CONNECTOR_KEY = "connector";
  public static final String FORMAT_KEY = "format";

  @NonNull Name name;
  @NonNull SqrlConfig config;
  @NonNull BaseTableConfig base;
  @NonNull DataSystemConnectorSettings connectorSettings;

  public TableConfig(@NonNull Name name, @NonNull SqrlConfig config) {
    this(name,config,getConnectorSettings(config));
  }

  public TableConfig(@NonNull Name name, @NonNull SqrlConfig config,
      @NonNull DataSystemConnectorSettings connectorSettings) {
    this(name, config, config.allAs(BaseTableConfig.class).get(), connectorSettings);
  }

  public TableConfig(@NonNull Name name, @NonNull SqrlConfig config, @NonNull BaseTableConfig baseTableConfig,
      @NonNull DataSystemConnectorSettings connectorSettings) {
    this.name = name;
    this.config = config;
    this.base = baseTableConfig;
    this.connectorSettings = connectorSettings;
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

  private static DataSystemConnectorSettings getConnectorSettings(SqrlConfig config) {
    SqrlConfig connectorConfig = config.getSubConfig(CONNECTOR_KEY);
    DataSystemConnectorFactory connectorFactory = DataSystemImplementationFactory.fromConfig(DataSystemConnectorFactory.class, connectorConfig);
    return connectorFactory.getSettings(connectorConfig);
  }


  public SqrlConfig getConnectorConfig() {
    return config.getSubConfig(CONNECTOR_KEY);
  }

  public DataSystemConnectorSettings getConnectorSettings() {
    return connectorSettings;
  }

  public String getConnectorName() {
    return getConnectorConfig().asString(DataSystemImplementationFactory.SYSTEM_NAME_KEY).get();
  }

  public Optional<String> getSchemaType() {
    if (hasFormat()) {
      Optional<String> schemaType = getFormat().getSchemaType();
      if (schemaType.isPresent()) return schemaType;
    }
    return Optional.ofNullable(base.getSchema());
  }

  public Optional<TableSchemaFactory> getSchemaFactory() {
    return getSchemaType().flatMap(f-> {
        try {
          return Optional.of(TableSchemaFactory.load(f));
        } catch (Exception e) {
          return Optional.empty();
        }
    });
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
    DataSystemConnectorSettings connector = getConnectorSettings();
    Name tableName = getName();
    return new TableSource(connector, this, basePath.concat(tableName), tableName, schema);
  }

  public TableInput initializeInput(NamePath basePath) {
    validateTable();
    getErrors().checkFatal(base.getType().isSource(), "Table is not a source: %s", name);
    DataSystemConnectorSettings connector = getConnectorSettings();
    Name tableName = getName();
    return new TableInput(connector, this, basePath.concat(tableName), tableName,
        Optional.empty());
  }

  public TableSink initializeSink(NamePath basePath,
      Optional<TableSchema> schema) {
    validateTable();
    getErrors().checkFatal(base.getType().isSink(), "Table is not a sink: %s", name);
    DataSystemConnectorSettings connector = getConnectorSettings();
    Name tableName = getName();
    return new TableSink(connector,this, basePath.concat(tableName), tableName, schema);
  }

  public DataSystemDiscovery initializeDiscovery() {
    DataSystemDiscoveryFactory factory = DataSystemImplementationFactory.fromConfig(DataSystemDiscoveryFactory.class, getConnectorConfig());
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

    public SqrlConfig getConfig() {
      return config;
    }

    public TableConfig build() {
      return new TableConfig(name,config);
    }

  }

  public Serialized serialize() {
    return new Serialized(name,config.serialize(), connectorSettings);
  }

  @AllArgsConstructor
  @Getter
  @NoArgsConstructor(force = true, access = AccessLevel.PRIVATE)
  @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "className")
  public static class Serialized {

    Name name;
    SerializedSqrlConfig config;
    DataSystemConnectorSettings connectorSettings;

    public TableConfig deserialize(ErrorCollector errors) {
      return new TableConfig(name, config.deserialize(errors), connectorSettings);
    }

  }

}
