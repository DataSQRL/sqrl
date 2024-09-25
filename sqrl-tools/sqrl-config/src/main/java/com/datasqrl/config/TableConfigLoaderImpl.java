package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.resolver.ResourceResolver;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import lombok.NonNull;

public class TableConfigLoaderImpl implements TableConfigLoader {
  @Override
  public TableConfig load(@NonNull Path path, @NonNull Name name,
      @NonNull ErrorCollector errors) {
    return SqrlConfigCommons.fromFilesTableConfig(name, errors, List.of(path));
  }
}
