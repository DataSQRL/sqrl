package ai.datasqrl.compile.loaders;

import ai.datasqrl.plan.local.generate.Resolve.Env;
import java.net.URI;

public class TypeLoader implements Loader {

  @Override
  public boolean handles(URI uri, String name) {
    return false;
  }

  @Override
  public void load(Env env, URI uri, String name) {

  }
}
