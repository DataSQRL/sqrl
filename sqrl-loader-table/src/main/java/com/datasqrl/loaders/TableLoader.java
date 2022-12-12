package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.tables.TableSource;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.schema.input.external.SchemaDefinition;
import java.nio.file.Path;
import java.util.Optional;
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
    return super.usesFile(file) || file.getFileName().toString().endsWith(
        DataSource.JSON_SCHEMA_FILE_SUFFIX) ||
        file.getFileName().toString().equals(DataSource.PACKAGE_SCHEMA_FILE);
  }

  @Override
  public boolean load(LoaderContext ctx, NamePath fullPath, Optional<Name> alias) {
    return readTable(ctx.getPackagePath(), fullPath, ctx.getErrorCollector()).map(
            tbl -> ctx.registerTable(tbl, alias))
        .map(name -> name != null).orElse(false);
  }

  public Optional<TableSource> readTable(Path rootDir, NamePath fullPath, ErrorCollector errors) {
    return DataSource.readTable(rootDir, fullPath, errors, TableSource.class, deserialize);
  }


  public SchemaDefinition loadPackageSchema(Path baseDir) {
    Path tableSchemaPath = baseDir.resolve(DataSource.PACKAGE_SCHEMA_FILE);
    return deserialize.mapYAMLFile(tableSchemaPath, SchemaDefinition.class);
  }

  @Override
  public boolean isPackage(Path packageBasePath, NamePath fullPath) {
    return AbstractLoader.isPackagePath(packageBasePath,fullPath);
  }
}
