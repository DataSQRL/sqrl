package com.datasqrl.io.tables;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.util.ServiceLoaderDiscovery;
import java.net.URI;

public interface TableSchemaFactory {
  TableSchema create(NamePath basePath, URI baseURI, ResourceResolver resourceResolver, TableConfig tableConfig, ErrorCollector errors);
  TableSchema create(String schemaDefinition, NameCanonicalizer nameCanonicalizer);
  String getSchemaFilename(TableConfig tableConfig);

  String getType();

  static TableSchemaFactory load(String schemaType) {
    return ServiceLoaderDiscovery.get(TableSchemaFactory.class, TableSchemaFactory::getType, schemaType);
  }

}
