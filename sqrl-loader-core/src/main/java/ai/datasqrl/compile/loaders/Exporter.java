package ai.datasqrl.compile.loaders;

import ai.datasqrl.io.sources.dataset.TableSink;
import ai.datasqrl.parse.tree.name.NamePath;

import java.util.Optional;

public interface Exporter {

    Optional<TableSink> export(LoaderContext ctx, NamePath fullPath);

}
