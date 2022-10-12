package ai.datasqrl.compile.loaders;

import static ai.datasqrl.compile.loaders.DataSourceLoader.resolveUri;

import ai.datasqrl.schema.input.external.SchemaDefinition;
import com.fasterxml.jackson.dataformat.yaml.YAMLMapper;
import java.net.URI;

public class DiscoveredSchemaLoader implements SchemaLoader {

  public SchemaDefinition resolve(URI uri, String name) {
    SchemaDefinition schemaDef = resolveUri(uri, name + ".schema.yml", new YAMLMapper(), SchemaDefinition.class);

    return schemaDef;
  }
}
