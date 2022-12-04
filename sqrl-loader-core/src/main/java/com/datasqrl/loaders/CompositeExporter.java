/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.io.tables.TableSink;
import com.datasqrl.name.NamePath;
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
      if (exporter.usesFile(file)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Optional<TableSink> export(LoaderContext ctx, NamePath fullPath) {
    for (Exporter exporter : exporters) {
      Optional<TableSink> result = exporter.export(ctx, fullPath);
      if (result.isPresent()) {
        return result;
      }
    }
    return Optional.empty();
  }
}
