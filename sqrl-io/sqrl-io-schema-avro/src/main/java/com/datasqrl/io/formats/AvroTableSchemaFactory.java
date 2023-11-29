package com.datasqrl.io.formats;

import com.datasqrl.canonicalizer.NameCanonicalizer;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableConfig;
import com.datasqrl.io.tables.TableSchema;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.serializer.Deserializer;
import com.datasqrl.util.BaseFileUtil;
import com.google.auto.service.AutoService;
import java.net.URI;
import java.util.Optional;
import org.apache.avro.Schema;

@AutoService(TableSchemaFactory.class)
public class AvroTableSchemaFactory implements TableSchemaFactory {

  public static final String SCHEMA_EXTENSION = ".avsc";

  public static final String SCHEMA_TYPE = "avro";

  @Override
  public Optional<TableSchema> create(
      NamePath basePath, URI baseURI, ResourceResolver resourceResolver, TableConfig tableConfig, ErrorCollector errors) {
    Optional<URI> schemaPath = resourceResolver
        .resolveFile(basePath.concat(NamePath.of(getSchemaFilename(tableConfig))));
    return schemaPath.map(s->
        create(BaseFileUtil.readFile(s), tableConfig.getBase().getCanonicalizer(), schemaPath));
  }

  @Override
  public TableSchema create(String schemaDefinition, NameCanonicalizer nameCanonicalizer) {
    return create(schemaDefinition, nameCanonicalizer, Optional.empty());
  }

  private TableSchema create(String schemaDefinition, NameCanonicalizer nameCanonicalizer, Optional<URI> location) {
    Schema schema = new Schema.Parser().parse(schemaDefinition);
    return new AvroSchemaHolder(schema, schemaDefinition, location);
  }


  @Override
  public String getSchemaFilename(TableConfig tableConfig) {
    return tableConfig.getName().getDisplay() + SCHEMA_EXTENSION;
  }

  @Override
  public String getType() {
    return SCHEMA_TYPE;
  }


}
