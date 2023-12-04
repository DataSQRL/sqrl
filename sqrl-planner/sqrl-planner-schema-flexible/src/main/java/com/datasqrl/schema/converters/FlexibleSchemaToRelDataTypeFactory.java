/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.schema.converters;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.input.FlexibleTableConverter;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.type.RelDataType;

@AutoService(SchemaToRelDataTypeFactory.class)
public class FlexibleSchemaToRelDataTypeFactory implements SchemaToRelDataTypeFactory {

  @Override
  public String getSchemaType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public RelDataType map(TableSchema schema, Name tableName, ErrorCollector errors) {
    Preconditions.checkArgument(schema instanceof FlexibleTableSchemaHolder);
    FlexibleTableSchemaHolder holder = (FlexibleTableSchemaHolder)schema;
    FlexibleTableConverter converter = new FlexibleTableConverter(holder.getSchema(), tableName);
    FlexibleTable2RelDataTypeConverter relDataTypeConverter = new FlexibleTable2RelDataTypeConverter();
    return converter.apply(relDataTypeConverter);
  }

}
