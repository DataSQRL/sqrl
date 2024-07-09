package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.resolver.ResourceResolver;
import java.net.URI;
import java.nio.file.Path;
import lombok.NonNull;

public class TableConfigLoaderImpl implements TableConfigLoader {
  @Override
  public TableConfig load(@NonNull Path path, @NonNull Name name,
      @NonNull ErrorCollector errors) {
    SqrlConfig config = SqrlConfigCommons.fromFiles(errors, path);
    return new TableConfigImpl(name, config);
  }
}
