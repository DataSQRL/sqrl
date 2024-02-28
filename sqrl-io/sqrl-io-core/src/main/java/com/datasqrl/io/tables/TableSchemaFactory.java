package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.util.ServiceLoaderDiscovery;
import com.google.common.base.Preconditions;
import java.net.URI;
import java.util.Optional;

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
  default TableSchema create(String schemaDefinition, ErrorCollector errors) {
    TableSchema schema = create(schemaDefinition, Optional.empty(), errors);
    Preconditions.checkArgument(!errors.hasErrors(), "Encountered errors processing internal schema: %s", errors);
    return schema;
  }
  TableSchema create(String schemaDefinition, Optional<URI> location, ErrorCollector errors);
  String getSchemaFilename(TableConfig tableConfig);

  String getType();

  static TableSchemaFactory load(String schemaType) {
    return ServiceLoaderDiscovery.get(TableSchemaFactory.class, TableSchemaFactory::getType, schemaType);
  }

}
