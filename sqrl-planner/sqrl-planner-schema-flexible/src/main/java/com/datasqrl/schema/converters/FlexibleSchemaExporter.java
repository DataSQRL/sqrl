package com.datasqrl.schema.converters;

import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.TableSchemaExporterFactory;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.schema.input.FlexibleTableSchemaFactory;
import com.google.auto.service.AutoService;


@AutoService(TableSchemaExporterFactory.class)
public class FlexibleSchemaExporter implements TableSchemaExporterFactory {

  @Override
  public TableSchema convert(UniversalTable tableSchema) {
    return UtbToFlexibleSchema.createFlexibleSchema(tableSchema);
  }

  @Override
  public String getType() {
    return FlexibleTableSchemaFactory.SCHEMA_TYPE;
  }
}
