package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.loaders.ResourceResolver;
import com.datasqrl.name.Name;
import com.datasqrl.name.NameCanonicalizer;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URL;
import java.util.Optional;
import java.util.Set;
import lombok.Builder;
import lombok.Value;

public interface TableSchemaFactory {
  Optional<TableSchema> create(URL url, SchemaFactoryContext context);

  String baseFileSuffix();

  /**
   * All files resolves by this factory
   */
  Set<String> allSuffixes();

  Optional<String> getFileName();


  @Value
  @Builder
  public class SchemaFactoryContext {
    ResourceResolver resourceResolver;
    ErrorCollector errors;
    NameCanonicalizer canonicalizer;
    ObjectMapper mapper;
    Name resolvedName;
  }
}
