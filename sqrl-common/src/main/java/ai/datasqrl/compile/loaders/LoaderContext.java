package ai.datasqrl.compile.loaders;

import ai.datasqrl.config.error.ErrorCollector;
import ai.datasqrl.function.builtin.time.FlinkFnc;
import ai.datasqrl.io.sources.dataset.TableSource;
import ai.datasqrl.parse.tree.name.Name;
import java.nio.file.Path;
import java.util.Optional;

public interface LoaderContext {

  public Path getPackagePath();

  public void addFunction(FlinkFnc flinkFnc);

  ErrorCollector getErrorCollector();

  Name registerTable(TableSource tbl, Optional<Name> alias);
}
