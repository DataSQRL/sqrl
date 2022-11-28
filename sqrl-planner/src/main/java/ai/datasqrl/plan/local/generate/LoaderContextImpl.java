package ai.datasqrl.plan.local.generate;

import ai.datasqrl.compile.loaders.LoaderContext;
import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.function.builtin.time.FlinkFnc;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.generate.Resolve.Env;
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
