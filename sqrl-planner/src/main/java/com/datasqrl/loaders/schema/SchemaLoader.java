package com.datasqrl.loaders.schema;

import com.datasqrl.io.schema.SchemaConversionResult;
import java.util.Optional;

@FunctionalInterface
public interface SchemaLoader {

  SchemaLoader NONE = (t, r) -> Optional.empty();

  Optional<SchemaConversionResult> loadSchema(String tableName, String relativeSchemaFilePath);
}
