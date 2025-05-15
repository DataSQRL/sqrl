package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.nio.file.Path;
import java.util.Optional;

public interface TableSchemaFactory {

  TableSchema create(String schemaDefinition, Optional<Path> location, ErrorCollector errors);

  String getType();

  String getExtension();

  static TableSchemaFactory loadByType(String schemaType) {
    return ServiceLoaderDiscovery.get(TableSchemaFactory.class, TableSchemaFactory::getType, schemaType);
  }

  static Optional<TableSchemaFactory> loadByExtension(String extension) {
    return ServiceLoaderDiscovery.findFirst(TableSchemaFactory.class, TableSchemaFactory::getExtension, extension);
  }

}
