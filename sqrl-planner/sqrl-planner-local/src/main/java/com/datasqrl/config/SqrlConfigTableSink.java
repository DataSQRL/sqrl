package com.datasqrl.config;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.loaders.LoaderUtil;
import com.datasqrl.loaders.ModuleLoader;
import com.google.inject.Inject;
import lombok.NonNull;
import lombok.experimental.Delegate;

public class SqrlConfigTableSink implements TableSink {

  @Delegate
  TableSink tableSink;
  @Inject
  public SqrlConfigTableSink(ModuleLoader moduleLoader, SqrlConfig config,
      ErrorCollector errors) {
    CompilerConfiguration compilerConfig = CompilerConfiguration.fromRootConfig(config);
    this.tableSink = loadErrorSink(moduleLoader, compilerConfig.getErrorSink(), errors);
  }

  private TableSink loadErrorSink(ModuleLoader moduleLoader, @NonNull String errorSinkName, ErrorCollector error) {
    NamePath sinkPath = NamePath.parse(errorSinkName);
    return LoaderUtil.loadSink(sinkPath, error, moduleLoader);
  }
}
