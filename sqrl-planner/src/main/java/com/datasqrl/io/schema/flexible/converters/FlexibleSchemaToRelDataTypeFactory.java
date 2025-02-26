/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.io.schema.flexible.converters;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.FlexibleTableConverter;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaFactory;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaHolder;
import com.datasqrl.io.tables.TableSchema;
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
  public RelDataType map(
      TableSchema schema, TableConfig tableConfig, Name tableName, ErrorCollector errors) {
    Preconditions.checkArgument(schema instanceof FlexibleTableSchemaHolder);
    FlexibleTableSchemaHolder holder = (FlexibleTableSchemaHolder) schema;
    FlexibleTableConverter converter = new FlexibleTableConverter(holder.getSchema(), tableName);
    FlexibleTable2RelDataTypeConverter relDataTypeConverter =
        new FlexibleTable2RelDataTypeConverter();
    return converter.apply(relDataTypeConverter);
  }
}
