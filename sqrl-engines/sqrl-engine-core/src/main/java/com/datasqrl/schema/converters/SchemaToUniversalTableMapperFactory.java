package com.datasqrl.schema.converters;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.DataSystemConnectorSettings;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.schema.UniversalTable;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.util.Optional;

public interface SchemaToUniversalTableMapperFactory {

  String getSchemaType();

  UniversalTable map(TableSchema schema, DataSystemConnectorSettings connectorSettings,
      Optional<Name> tblAlias);

  public static SchemaToUniversalTableMapperFactory load(TableSchema schema) {
    return ServiceLoaderDiscovery.get(SchemaToUniversalTableMapperFactory.class,
        SchemaToUniversalTableMapperFactory::getSchemaType, schema.getSchemaType());
  }


}
