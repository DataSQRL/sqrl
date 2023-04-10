/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.tables;

import com.datasqrl.io.DataSystemConnector;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.input.InputTableSchema;
import com.datasqrl.schema.input.SchemaValidator;
import lombok.Getter;
import lombok.NonNull;

/**
 * A {@link TableSource} defines an input source to be imported into an SQML script. A
 * {@link TableSource} is comprised of records and is the smallest unit of data that one can refer
 * to within an SQRL script.
 */
@Getter
public class TableSource extends TableInput {

  @NonNull
  private final TableSchema tableSchema;
  private final SchemaValidator validator;

  public TableSource(DataSystemConnector dataset, TableConfig configuration, NamePath path,
      Name name, TableSchema schema, SchemaValidator validator) {
    super(dataset, configuration, path, name);
    this.tableSchema = schema;
    this.validator = validator;
//    this.statistic = TableStatistic.of(1000); //TODO: extract from schema
  }

  public InputTableSchema getSchema() {
    return new InputTableSchema(tableSchema, connector.hasSourceTimestamp());
  }

  @Override
  public SchemaValidator getSchemaValidator() {
    return validator;
  }
}
