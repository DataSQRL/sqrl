package com.datasqrl;

import com.datasqrl.io.tables.TableSource;
import com.datasqrl.loaders.Loader;
import com.datasqrl.loaders.LoaderContext;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;

public class JsonSchemaLoader implements Loader {

  public static final String SCHEMA_JSON = ".schema.json";
  public static final String TABLE_JSON = ".table.json";

  //Loads:

  @Override
  public boolean usesFile(Path file) {
    return file.getFileName().toString().endsWith(SCHEMA_JSON) ||
        file.getFileName().toString().endsWith(TABLE_JSON);
  }

  @Override
  public Optional<String> loadsFile(Path file) {
    return Optional.empty();
  }

  @Override
  public boolean load(LoaderContext ctx, NamePath fullPath, Optional<Name> alias) {
    ctx.registerTable(new TableSource(), alias);


    return false;
  }

  @Override
  public Collection<Name> loadAll(LoaderContext ctx, NamePath basePath) {
    return null;
  }
}
