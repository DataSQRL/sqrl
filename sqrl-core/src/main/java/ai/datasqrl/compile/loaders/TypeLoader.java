package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import java.net.URI;
import java.util.Optional;

public class TypeLoader implements Loader {

  @Override
  public boolean handles(URI uri, String name) {
    return false;
  }

  @Override
  public boolean handlesFile(URI uri, String name) {
    return false;
  }

  @Override
  public void load(Env env, URI uri, String name, Optional<Name> alias) {

  }

  @Override
  public void loadFile(Env env, URI uri, String name) {

  }
}
