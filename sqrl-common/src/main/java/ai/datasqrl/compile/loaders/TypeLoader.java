package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve.Env;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.Optional;

public class TypeLoader implements Loader {

  @Override
  public Optional<String> handles(Path file) {
    return Optional.empty();
  }

  @Override
  public boolean load(Env env, NamePath fullPath, Optional<Name> alias) {
    return false;
  }

  @Override
  public Collection<Name> loadAll(Env env, NamePath basePath) {
    return Collections.EMPTY_LIST;
  }
}
