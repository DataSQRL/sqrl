package com.datasqrl.compile.loaders;

import com.datasqrl.config.error.ErrorCollector;
import com.datasqrl.function.builtin.time.FlinkFnc;
import com.datasqrl.io.sources.dataset.TableSource;
import com.datasqrl.parse.tree.name.Name;

import java.nio.file.Path;
import java.util.Optional;

public interface LoaderContext {

  public Path getPackagePath();

  public void addFunction(FlinkFnc flinkFnc);

  ErrorCollector getErrorCollector();

  Name registerTable(TableSource table, Optional<Name> alias);
}
