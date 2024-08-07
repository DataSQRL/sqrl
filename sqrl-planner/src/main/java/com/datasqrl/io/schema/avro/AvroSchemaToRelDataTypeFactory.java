package com.datasqrl.io.schema.avro;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.tables.TableSchema;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;

@AutoService(SchemaToRelDataTypeFactory.class)
public class AvroSchemaToRelDataTypeFactory implements SchemaToRelDataTypeFactory {

  @Override
  public String getSchemaType() {
    return AvroTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public RelDataType map(TableSchema schema, Name tableName, ErrorCollector errors) {
    Preconditions.checkArgument(schema instanceof AvroSchemaHolder);
    Schema avroSchema = ((AvroSchemaHolder)schema).getSchema();
    AvroToRelDataTypeConverter converter = new AvroToRelDataTypeConverter(errors);
    return converter.convert(avroSchema);
  }


}
