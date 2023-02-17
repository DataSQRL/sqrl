package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.Deserializer;

import java.net.URI;
import java.util.Optional;

public interface TableSchemaFactory {
  Optional<TableSchema> create(URI baseURI, TableConfig tableConfig, Deserializer deserializer, ErrorCollector errors);

  public String getType();

}
