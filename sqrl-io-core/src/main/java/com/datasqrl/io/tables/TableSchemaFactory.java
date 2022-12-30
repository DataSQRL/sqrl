package com.datasqrl.io.tables;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import java.util.Optional;
import java.util.Set;

public interface TableSchemaFactory {
  Optional<TableSchema> create(com.datasqrl.loaders.Deserializer deserialize, Path baseDir, TableConfig tableConfig,
      ErrorCollector errors);

  String baseFileSuffix();

  /**
   * All files resolves by this factory
   */
  Set<String> allSuffixes();

  Optional<String> getFileName();
}
