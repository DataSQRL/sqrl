package ai.datasqrl.compile.loaders;

import ai.datasqrl.io.sources.dataset.TableSink;
import ai.datasqrl.parse.tree.name.NamePath;

import java.nio.file.Path;
import java.util.Optional;

public interface Exporter {

    boolean usesFile(Path file);

    Optional<TableSink> export(LoaderContext ctx, NamePath fullPath);

}
