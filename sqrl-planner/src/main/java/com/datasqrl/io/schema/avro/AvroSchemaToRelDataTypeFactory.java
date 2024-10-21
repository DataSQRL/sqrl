package com.datasqrl.io.schema.avro;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory;
import com.datasqrl.io.tables.TableSchema;
import com.google.auto.service.AutoService;
import com.google.common.base.Preconditions;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.calcite.rel.type.RelDataType;

@AutoService(SchemaToRelDataTypeFactory.class)
@Slf4j
public class AvroSchemaToRelDataTypeFactory implements SchemaToRelDataTypeFactory {

  @Override
  public String getSchemaType() {
    return AvroTableSchemaFactory.SCHEMA_TYPE;
  }

  @Override
  public RelDataType map(TableSchema schema, TableConfig tableConfig, Name tableName, ErrorCollector errors) {
    Preconditions.checkArgument(schema instanceof AvroSchemaHolder);
    Schema avroSchema = ((AvroSchemaHolder)schema).getSchema();

    boolean legacyTimestampMapping = getLegacyTimestampMapping(tableConfig);


    AvroToRelDataTypeConverter converter = new AvroToRelDataTypeConverter(errors, legacyTimestampMapping);
    return converter.convert(avroSchema);
  }

  private boolean getLegacyTimestampMapping(TableConfig tableConfig) {
    if (tableConfig == null) return true;
    Map<String, Object> configMap = tableConfig.getConnectorConfig().toMap();
    Object legacy = configMap.get("avro.timestamp_mapping.legacy");
    if (legacy instanceof Boolean) {
      return (Boolean) legacy;
    } else if (legacy != null) {
      log.warn("Expected boolean type for 'timestamp_mapping.legacy'");
    }

    log.warn("Avro value 'timestamp_mapping.legacy' is not set to 'false'. "
        + "This should be added to your table config for flink version 1.19 and later.");
    return true; //default value
  }
}
