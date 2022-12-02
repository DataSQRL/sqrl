package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;

public interface Loader {

  default boolean usesFile(Path file) {
    return loadsFile(file).isPresent();
  }

  Optional<String> loadsFile(Path file);

  boolean load(LoaderContext ctx, NamePath fullPath, Optional<Name> alias);

  Collection<Name> loadAll(LoaderContext ctx, NamePath basePath);

}
