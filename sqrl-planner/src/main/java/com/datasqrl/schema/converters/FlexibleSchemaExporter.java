package com.datasqrl.schema.converters;

import com.datasqrl.schema.TableSchemaExporterFactory;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.datasqrl.schema.input.FlexibleTableSchemaHolder;
import com.google.auto.service.AutoService;
import org.apache.calcite.rel.type.RelDataTypeField;


@AutoService(TableSchemaExporterFactory.class)
public class FlexibleSchemaExporter implements TableSchemaExporterFactory {

  @Override
  public FlexibleTableSchemaHolder convert(RelDataTypeField tableType) {
    return RelDataTypeToFlexibleSchema.createFlexibleSchema(tableType);
  }

  @Override
  public String getType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }
}
