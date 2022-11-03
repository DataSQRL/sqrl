package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve.Env;

import java.nio.file.Path;
import java.util.Collection;
import java.util.Optional;

public interface Loader {

//  boolean handles(URI uri, String name);
//  boolean handlesFile(URI uri, String name);
//
//  void load(Env env, URI uri, String name, Optional<Name> alias);
//  void loadFile(Env env, URI uri, String name);

  Optional<String> handles(Path file);

  boolean load(Env env, NamePath fullPath, Optional<Name> alias);

  Collection<Name> loadAll(Env env, NamePath basePath);

}
