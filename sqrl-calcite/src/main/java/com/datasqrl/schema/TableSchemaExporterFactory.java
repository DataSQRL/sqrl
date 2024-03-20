package com.datasqrl.schema;

import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.util.ServiceLoaderDiscovery;
import org.apache.calcite.rel.type.RelDataTypeField;


public interface TableSchemaExporterFactory {

  TableSchema convert(RelDataTypeField tableType);

  String getType();

  static TableSchemaExporterFactory load(String schemaType) {
    return ServiceLoaderDiscovery.get(TableSchemaExporterFactory.class, TableSchemaExporterFactory::getType, schemaType);
  }

}
