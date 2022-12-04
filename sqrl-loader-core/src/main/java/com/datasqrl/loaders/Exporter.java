package com.datasqrl.loaders;

import com.datasqrl.io.tables.TableSink;
import com.datasqrl.name.NamePath;

import java.nio.file.Path;
import java.util.Optional;

public interface Exporter {

  boolean usesFile(Path file);

  Optional<TableSink> export(LoaderContext ctx, NamePath fullPath);

}
