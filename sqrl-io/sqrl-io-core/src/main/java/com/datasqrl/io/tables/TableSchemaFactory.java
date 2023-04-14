package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.model.schema.SchemaDefinition;
import com.datasqrl.serializer.Deserializer;

import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.canonicalizer.NamePath;
import java.net.URI;
import java.util.Optional;

public interface TableSchemaFactory {
  Optional<TableSchema> create(NamePath basePath, URI baseURI, ResourceResolver resourceResolver, TableConfig tableConfig, Deserializer deserializer, ErrorCollector errors);
  Optional<TableSchema> create(SchemaDefinition definition);

  public String getType();

}
