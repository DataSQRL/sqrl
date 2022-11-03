package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve.Env;

import java.util.Collections;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * All jars are loaded on the class path and resolved with java's ServiceLoader.
 *
 */
public class JavaFunctionLoader implements Loader {

  private static final Pattern PATTERN = Pattern.compile(".*\\.jar$");

  @Override
  public boolean load(Env env, NamePath fullPath, Optional<Name> alias) {
    return false;
//    Preconditions.checkState(alias.isEmpty(), "Alias for functions not yet supported");
//    ServiceLoader<SqlFunction> serviceLoader = ServiceLoader.load(SqlFunction.class);
//    for (SqlFunction function : serviceLoader) {
//      SqrlOperatorTable.instance().register(function);
//    }
//    throw new RuntimeException("Functions not yet supported");
  }

  @Override
  public Set<Name> loadAll(Env env, NamePath basePath) {
    //"Function loading from entire jar tbd"
    return Collections.EMPTY_SET;
  }
}
