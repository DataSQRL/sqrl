package com.datasqrl.config;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.resolver.ResourceResolver;
import java.net.URI;
import lombok.NonNull;

public class TableConfigLoaderImpl implements TableConfigLoader {
  @Override
  public TableConfig load(@NonNull URI uri, @NonNull Name name,
      @NonNull ErrorCollector errors) {
    SqrlConfig config = SqrlConfigCommons.fromURL(errors, ResourceResolver.toURL(uri));
    return new TableConfigImpl(name, config);
  }
}
