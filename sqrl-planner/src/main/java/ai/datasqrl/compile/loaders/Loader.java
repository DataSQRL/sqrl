package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve.Env;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;

public interface Loader {

  Optional<String> handles(Path file);

  boolean load(Env env, NamePath fullPath, Optional<Name> alias);

  Collection<Name> loadAll(Env env, NamePath basePath);

}
