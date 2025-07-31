package com.datasqrl.loaders.schema;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.io.schema.flexible.converters.SchemaToRelDataTypeFactory.SchemaResult;
import java.util.Optional;

@FunctionalInterface
public interface SchemaLoader {

  SchemaLoader NONE = (t, r) -> Optional.empty();

  Optional<SchemaResult> loadSchema(String tableName, String relativeSchemaFilePath);
}
