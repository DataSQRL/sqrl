package com.datasqrl.schema.converters;

import com.datasqrl.calcite.type.NamedRelDataType;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.google.auto.service.AutoService;


@AutoService(TableSchemaExporterFactory.class)
public class FlexibleSchemaExporter implements TableSchemaExporterFactory {

  @Override
  public FlexibleTableSchemaHolder convert(NamedRelDataType tableType) {
    return RelDataTypeToFlexibleSchema.createFlexibleSchema(tableType);
  }

  @Override
  public String getType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }
}
