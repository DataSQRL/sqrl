package ai.datasqrl.compile.loaders;

import ai.datasqrl.plan.local.generate.Resolve.Env;
import java.net.URI;

public interface Loader {

  boolean handles(URI uri, String name);

  void load(Env env, URI uri, String name);
}
