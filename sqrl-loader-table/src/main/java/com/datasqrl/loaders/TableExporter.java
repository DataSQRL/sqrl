package com.datasqrl.loaders;

import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

public class TableExporter extends DataSource implements com.datasqrl.loaders.Exporter {

  private final Deserializer deserialize = new Deserializer();

  @Override
  public boolean isPackage(Path packageBasePath, NamePath fullPath) {
    return AbstractLoader.isPackagePath(packageBasePath, fullPath);
  }

  @Override
  public boolean usesFile(Path file) {
    return file.getFileName().toString().endsWith(JSON_SCHEMA_FILE_SUFFIX) ||
        file.getFileName().toString().equals(PACKAGE_SCHEMA_FILE) ||
        file.getFileName().toString().endsWith(TABLE_FILE_SUFFIX) ||
        file.getFileName().toString().equals(DATASYSTEM_FILE);
  }

  @Override
  public Optional<TableSink> export(LoaderContext ctx, NamePath fullPath) {
    Optional<TableSink> sink = readTable(ctx.getPackagePath(), fullPath, ctx.getErrorCollector(),
        TableSink.class, deserialize);
    if (sink.isEmpty()) {
      //See if we can discover the sink on-demand
      NamePath basePath = fullPath.subList(0, fullPath.size() - 1);
      Name tableName = fullPath.getLast();
      Path baseDir = AbstractLoader.namepath2Path(ctx.getPackagePath(), basePath);
      Path datasystempath = baseDir.resolve(DATASYSTEM_FILE);
      DataSystemConfig discoveryConfig;

      if (Files.isRegularFile(datasystempath)) {
        discoveryConfig = deserialize.mapJsonFile(datasystempath, DataSystemConfig.class);
      } else if (basePath.size() == 1 && basePath.getLast().getCanonical()
          .equals(PrintDataSystem.SYSTEM_TYPE)) {
        discoveryConfig = PrintDataSystem.DEFAULT_DISCOVERY_CONFIG;
      } else {
        return Optional.empty();
      }
      DataSystem dataSystem = discoveryConfig.initialize(ctx.getErrorCollector());
      if (dataSystem == null) {
        return Optional.empty();
      }
      return dataSystem.discoverSink(tableName, ctx.getErrorCollector()).map(tblConfig ->
          tblConfig.initializeSink(ctx.getErrorCollector(), basePath, Optional.empty()));
    } else {
      return sink;
    }
  }

}
