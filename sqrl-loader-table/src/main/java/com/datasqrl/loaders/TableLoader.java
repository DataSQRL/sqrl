/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSchemaFactory;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.input.external.SchemaDefinition;
import java.nio.file.Path;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Matcher;

public class TableLoader extends AbstractLoader {

  @Override
  public Optional<String> loadsFile(Path file) {
    Matcher matcher = DataSource.CONFIG_FILE_PATTERN.matcher(file.getFileName().toString());
    if (matcher.find()) {
      return Optional.of(matcher.group(1));
    }
    return Optional.empty();
  }

  @Override
  public boolean usesFile(Path file) {
    Optional schemaFile = ServiceLoader.load(TableSchemaFactory.class)
        .stream()
        .flatMap(f->f.get().allSuffixes().stream())
        .filter(s->file.getFileName().toString().endsWith(s))
        .findAny();

    return super.usesFile(file) || schemaFile.isPresent();
  }

  @Override
  public boolean load(LoaderContext ctx, NamePath fullPath, Optional<Name> alias) {
    return readTable(ctx.getPackagePath(), fullPath, ctx.getErrorCollector()).map(
            tbl -> ctx.registerTable(tbl, alias))
        .map(name -> name != null).orElse(false);
  }

  public Optional<TableSource> readTable(Path rootDir, NamePath fullPath, ErrorCollector errors) {
    return new DataSource().readTable(rootDir, fullPath, errors, TableSource.class, deserialize);
  }

  @Override
  public boolean isPackage(Path packageBasePath, NamePath fullPath) {
    return AbstractLoader.isPackagePath(packageBasePath,fullPath);
  }
}
