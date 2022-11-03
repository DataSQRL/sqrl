package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve.Env;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;

public class TypeLoader implements Loader {

  @Override
  public boolean load(Env env, NamePath fullPath, Optional<Name> alias) {
    return false;
  }

  @Override
  public Set<Name> loadAll(Env env, NamePath basePath) {
    return Collections.EMPTY_SET;
  }
}
