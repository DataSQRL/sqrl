package ai.datasqrl.compile.loaders;

import ai.datasqrl.schema.input.external.SchemaDefinition;
import java.net.URI;

public interface SchemaLoader {

  SchemaDefinition resolve(URI uri, String name);
}
