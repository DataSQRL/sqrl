package com.datasqrl.io.schema.flexible.converters;

import com.datasqrl.io.schema.flexible.FlexibleTableSchemaFactory;
import com.datasqrl.io.schema.flexible.FlexibleTableSchemaHolder;
import com.datasqrl.schema.TableSchemaExporterFactory;
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
