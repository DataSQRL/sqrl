package com.datasqrl.io.schema.flexible.converters;

import org.apache.calcite.rel.type.RelDataType;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.util.ServiceLoaderDiscovery;

public interface SchemaToRelDataTypeFactory {

  String getSchemaType();

  RelDataType map(TableSchema schema, TableConfig tableConfig, Name tableName, ErrorCollector errors);

  static SchemaToRelDataTypeFactory load(TableSchema schema) {
    return ServiceLoaderDiscovery.get(SchemaToRelDataTypeFactory.class,
        SchemaToRelDataTypeFactory::getSchemaType, schema.getSchemaType());
  }


}
