package ai.datasqrl.compile.loaders;

import ai.datasqrl.io.sources.dataset.TableSink;
import ai.datasqrl.parse.tree.name.NamePath;
import lombok.AllArgsConstructor;

import java.nio.file.Path;
import java.util.List;
import java.util.Optional;

@AllArgsConstructor
public class CompositeExporter implements Exporter {

    List<Exporter> exporters;

    @Override
    public boolean usesFile(Path file) {
        for (Exporter exporter : exporters) {
            if (exporter.usesFile(file)) return true;
        }
        return false;
    }

    @Override
    public Optional<TableSink> export(LoaderContext ctx, NamePath fullPath) {
        for (Exporter exporter : exporters) {
            Optional<TableSink> result = exporter.export(ctx, fullPath);
            if (result.isPresent()) return result;
        }
        return Optional.empty();
    }
}
