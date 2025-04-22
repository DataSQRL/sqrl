package com.datasqrl.config;

import java.nio.file.Path;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;

import lombok.NonNull;

public interface TableConfigLoader {

  TableConfig load(@NonNull Path path, @NonNull Name name,
      @NonNull ErrorCollector errors);
}
