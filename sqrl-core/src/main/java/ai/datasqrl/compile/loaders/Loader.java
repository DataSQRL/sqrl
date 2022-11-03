package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve.Env;

import java.util.Optional;
import java.util.Set;

public interface Loader {

//  boolean handles(URI uri, String name);
//  boolean handlesFile(URI uri, String name);
//
//  void load(Env env, URI uri, String name, Optional<Name> alias);
//  void loadFile(Env env, URI uri, String name);

  boolean load(Env env, NamePath fullPath, Optional<Name> alias);
  Set<Name> loadAll(Env env, NamePath basePath);

}
