/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.io.DataSystemConnectorConfig;
import com.datasqrl.io.ExternalDataType;
import com.datasqrl.io.SharedConfiguration;
import com.datasqrl.io.formats.FormatConfiguration;
import com.datasqrl.io.impl.CanonicalizerConfiguration;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.schema.input.SchemaAdjustmentSettings;
import com.datasqrl.schema.input.SchemaValidator;
import com.datasqrl.util.ConfigurationUtil;
import com.datasqrl.util.constraints.OptionalMinString;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;
import lombok.experimental.SuperBuilder;

import javax.validation.Valid;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import java.io.Serializable;
import java.util.Optional;

@SuperBuilder(toBuilder = true)
@NoArgsConstructor
@Getter
public class TableConfig extends SharedConfiguration implements Serializable {

  @NonNull @NotNull
  @Size(min = 3)
  String name;
  @OptionalMinString
  String identifier;
  @OptionalMinString
  String schema;
  @Valid @NonNull @NotNull
  DataSystemConnectorConfig connector;

  public TableConfig(@NonNull ExternalDataType type,
      @NonNull CanonicalizerConfiguration canonicalizer, @NonNull String charset,
      FormatConfiguration format, @NonNull String name, String identifier,
      String schema, @NonNull DataSystemConnectorConfig connector) {
    super(type, canonicalizer, charset, format);
    this.name = name;
    this.identifier = identifier;
    this.schema = schema;
    this.connector = connector;
  }

  /**
   * TODO: make this configurable
   *
   * @return
   */
  @JsonIgnore
  public SchemaAdjustmentSettings getSchemaAdjustmentSettings() {
    return SchemaAdjustmentSettings.DEFAULT;
  }

  private DataSystemConnector baseInitialize(ErrorCollector errors) {
    if (!Name.validName(name)) {
      errors.fatal("Table needs to have valid name: %s", name);
      return null;
    }
    errors = errors.resolve(name);
    if (!rootInitialize(errors)) {
      return null;
    }
    if (!ConfigurationUtil.javaxValidate(this, errors)) {
      return null;
    }

    if (Strings.isNullOrEmpty(identifier)) {
      identifier = name;
    }
    identifier = getCanonicalizer().getCanonicalizer().getCanonical(identifier);

    if (!format.initialize(errors.resolve("format"))) {
      return null;
    }

    DataSystemConnector connector = this.connector.initialize(
        errors.resolve(name).resolve("datasource"));
    if (connector == null) {
      return null;
    }
    if (connector.requiresFormat(getType()) && getFormat() == null) {
      errors.fatal("Need to configure a format");
      return null;
    }
    return connector;
  }

  public TableSource initializeSource(ErrorCollector errors, NamePath basePath,
      TableSchema schema) {
    DataSystemConnector connector = baseInitialize(errors);
    if (connector == null) {
      return null;
    }
    Preconditions.checkArgument(getType().isSource());
    Name tableName = getResolvedName();

    SchemaValidator validator = schema.getValidator(this.getSchemaAdjustmentSettings(), connector.hasSourceTimestamp());

    return new TableSource(connector, this, basePath.concat(tableName), tableName, schema, validator);
  }

  public TableInput initializeInput(ErrorCollector errors, NamePath basePath) {
    DataSystemConnector connector = baseInitialize(errors);
    if (connector == null) {
      return null;
    }
    Preconditions.checkArgument(getType().isSource());
    Name tableName = getResolvedName();
    return new TableInput(connector, this, basePath.concat(tableName), tableName);
  }

  public TableSink initializeSink(ErrorCollector errors, NamePath basePath,
      Optional<TableSchema> schema) {
    DataSystemConnector connector = baseInitialize(errors);
    if (connector == null) {
      return null;
    }
    Preconditions.checkArgument(getType().isSink());
    Name tableName = getResolvedName();
    return new TableSink(connector,  getConnector(),this, basePath.concat(tableName), tableName, schema);
  }

  @JsonIgnore
  public Name getResolvedName() {
    return Name.of(name, getCanonicalizer().getCanonicalizer());
  }

  public static TableConfigBuilder copy(SharedConfiguration config) {
    return TableConfig.builder()
        .type(config.getType())
        .canonicalizer(config.getCanonicalizer())
        .charset(config.getCharset())
        .format(config.getFormat());
  }

}
