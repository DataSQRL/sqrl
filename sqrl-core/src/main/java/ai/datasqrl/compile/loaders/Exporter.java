package ai.datasqrl.compile.loaders;

import ai.datasqrl.io.sources.dataset.TableSink;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve;

import java.util.Optional;

public interface Exporter {

    Optional<TableSink> export(Resolve.Env env, NamePath fullPath);

}
