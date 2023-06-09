package com.datasqrl.schema;

import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.util.ServiceLoaderDiscovery;


public interface TableSchemaExporterFactory {

  TableSchema convert(UniversalTable tableSchema);

  String getType();

  static TableSchemaExporterFactory load(String schemaType) {
    return ServiceLoaderDiscovery.get(TableSchemaExporterFactory.class, TableSchemaExporterFactory::getType, schemaType);
  }

}
