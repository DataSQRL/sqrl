package com.datasqrl.io.tables;

import java.nio.file.Path;
import java.util.Optional;

import com.datasqrl.config.TableConfig;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Preconditions;

public interface TableSchemaFactory {

  /**
   * This method should only be used in the executable plan after the schema has already been
   * validated.
   *
   * @param schemaDefinition
   * @param errors
   * @return
   * @deprecated Should not be used anymore and will be removed once factored out of Flink
   * executable plan
   */
  @Deprecated
default TableSchema create(String schemaDefinition, ErrorCollector errors) {
    var schema = create(schemaDefinition, Optional.empty(), errors);
    Preconditions.checkArgument(!errors.hasErrors(), "Encountered errors processing internal schema: %s", errors);
    return schema;
  }
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
