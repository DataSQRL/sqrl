/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.input;

import com.datasqrl.engine.stream.flink.RowMapperFactory;
import com.datasqrl.io.stats.DefaultSchemaGenerator;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.name.Name;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.constraint.Constraint;
import com.datasqrl.schema.converters.FlexibleSchemaRowMapper;
import com.datasqrl.schema.converters.RowConstructor;
import com.datasqrl.schema.converters.RowMapper;
import com.google.auto.service.AutoService;
import lombok.*;

import java.util.Collections;
import java.util.List;

@Getter
@ToString(callSuper = true)
@NoArgsConstructor
@EqualsAndHashCode(callSuper = true)
@AutoService(TableSchema.class)
public class FlexibleTableSchema extends FlexibleFieldSchema implements TableSchema {

  private boolean isPartialSchema;
  @NonNull
  private RelationType<Field> fields;
  @NonNull
  private List<Constraint> constraints;

  public FlexibleTableSchema(Name name, SchemaElementDescription description, Object default_value,
                             boolean isPartialSchema, RelationType<Field> fields, List<Constraint> constraints) {
    super(name, description, default_value);
    this.isPartialSchema = isPartialSchema;
    this.fields = fields;
    this.constraints = constraints;
  }

  @Override
  public RowMapper getRowMapper(RowConstructor rowConstructor,
                                boolean hasSourceTimestamp) {

    return new FlexibleSchemaRowMapper(this, hasSourceTimestamp,
            rowConstructor);
  }

  @Override
  public String getSchemaType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public SchemaValidator getValidator(TableConfig tableConfig, boolean hasSourceTimestamp) {

    InputTableSchema inputTableSchema = new InputTableSchema(this, hasSourceTimestamp);
    DefaultSchemaValidator validator = new DefaultSchemaValidator(inputTableSchema,
            tableConfig.getSchemaAdjustmentSettings(),
            tableConfig.getNameCanonicalizer(),
            new DefaultSchemaGenerator(tableConfig.getSchemaAdjustmentSettings()));
    return validator;
  }

  @Override
  public UniversalTable createUniversalTable(boolean hasSourceTimestamp) {
    return RowMapperFactory.getFlexibleUniversalTableBuilder(this, hasSourceTimestamp);
  }

  @Setter
  public static class Builder extends FlexibleFieldSchema.Builder {

    protected boolean isPartialSchema = true;
    protected RelationType<Field> fields;
    protected List<Constraint> constraints = Collections.EMPTY_LIST;

    public void copyFrom(FlexibleTableSchema f) {
      super.copyFrom(f);
      isPartialSchema = f.isPartialSchema;
      fields = f.fields;
      constraints = f.constraints;
    }

    public FlexibleTableSchema build() {
      return new FlexibleTableSchema(name, description, default_value, isPartialSchema, fields,
              constraints);
    }

  }

  public static FlexibleTableSchema empty(Name name) {
    Builder b = new Builder();
    b.setName(name);
    b.setFields(RelationType.EMPTY);
    return b.build();
  }




}
