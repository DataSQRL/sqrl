package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.model.schema.SchemaDefinition;

import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.util.ServiceLoaderDiscovery;

import java.net.URI;
import java.util.Optional;

public interface TableSchemaFactory {
  TableSchema create(NamePath basePath, URI baseURI, ResourceResolver resourceResolver, TableConfig tableConfig, ErrorCollector errors);
  TableSchema create(String schemaDefinition, NameCanonicalizer nameCanonicalizer);

  String getType();

  static TableSchemaFactory load(String schemaType) {
    return ServiceLoaderDiscovery.get(TableSchemaFactory.class, TableSchemaFactory::getType, schemaType);
  }

}
