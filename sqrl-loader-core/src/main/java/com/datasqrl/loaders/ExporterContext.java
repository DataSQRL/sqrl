package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import java.nio.file.Path;
import lombok.Value;

public interface ExporterContext {

  public Path getPackagePath();

  ErrorCollector getErrorCollector();

  @Value
  public static class Implementation implements ExporterContext {

    Path packagePath;
    ErrorCollector errorCollector;

  }

}
