package com.datasqrl.schema;

import com.datasqrl.calcite.type.NamedRelDataType;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.util.ServiceLoaderDiscovery;
import org.apache.calcite.rel.type.RelDataType;


public interface TableSchemaExporterFactory {

  TableSchema convert(NamedRelDataType tableType);

  String getType();

  static TableSchemaExporterFactory load(String schemaType) {
    return ServiceLoaderDiscovery.get(TableSchemaExporterFactory.class, TableSchemaExporterFactory::getType, schemaType);
  }

}
