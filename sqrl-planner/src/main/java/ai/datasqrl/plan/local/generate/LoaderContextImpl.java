package com.datasqrl.plan.local.generate;

import com.datasqrl.compile.loaders.LoaderContext;
import com.datasqrl.config.error.ErrorCollector;
import com.datasqrl.function.builtin.time.FlinkFnc;
import com.datasqrl.io.sources.dataset.TableSource;
import com.datasqrl.parse.tree.name.Name;
import com.datasqrl.plan.local.generate.Resolve.Env;
import lombok.Value;

import java.nio.file.Path;
import java.util.Optional;

@Value
class LoaderContextImpl implements LoaderContext {

  Env env;

  @Override
  public Path getPackagePath() {
    return env.getPackagePath();
  }

  @Override
  public void addFunction(FlinkFnc flinkFnc) {
    env.getResolvedFunctions().add(flinkFnc);
  }

  @Override
  public ErrorCollector getErrorCollector() {
    return env.errors;
  }

  @Override
  public Name registerTable(TableSource tbl, Optional<Name> alias) {
    return env.registerTable(tbl, alias);
  }
}
