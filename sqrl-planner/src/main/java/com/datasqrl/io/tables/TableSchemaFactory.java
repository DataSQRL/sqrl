package com.datasqrl.io.tables;

import java.nio.file.Path;
import java.util.Optional;

import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Preconditions;

public interface TableSchemaFactory {

  TableSchema create(String schemaDefinition, Optional<Path> location, ErrorCollector errors);
  String getSchemaFilename(TableConfig tableConfig);

  String getType();

  String getExtension();

  static TableSchemaFactory loadByType(String schemaType) {
    return ServiceLoaderDiscovery.get(TableSchemaFactory.class, TableSchemaFactory::getType, schemaType);
  }

  static Optional<TableSchemaFactory> loadByExtension(String extension) {
    return ServiceLoaderDiscovery.findFirst(TableSchemaFactory.class, TableSchemaFactory::getExtension, extension);
  }

}
