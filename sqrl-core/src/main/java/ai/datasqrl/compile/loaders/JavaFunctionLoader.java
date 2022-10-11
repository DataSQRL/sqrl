package ai.datasqrl.compile.loaders;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.plan.calcite.SqrlOperatorTable;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import com.google.common.base.Preconditions;
import java.io.File;
import java.net.URI;
import java.util.Optional;
import java.util.ServiceLoader;
import java.util.regex.Pattern;
import org.apache.calcite.sql.SqlFunction;

/**
 * All jars are loaded on the class path and resolved with java's ServiceLoader.
 *
 */
public class JavaFunctionLoader implements Loader {
  private static final Pattern PATTERN = Pattern.compile(".*\\.jar");

  @Override
  public boolean handles(URI uri, String name) {
    return new File(uri.resolve(name + ".jar")).exists();
  }

  @Override
  public boolean handlesFile(URI uri, String name) {
    File file = new File(uri.resolve(name));
    return PATTERN.matcher(name).find() && file.exists();
  }

  @Override
  public void load(Env env, URI uri, String name, Optional<Name> alias) {
    Preconditions.checkState(alias.isEmpty(), "Alias for functions not yet supported");
    ServiceLoader<SqlFunction> serviceLoader = ServiceLoader.load(SqlFunction.class);
    for (SqlFunction function : serviceLoader) {
      SqrlOperatorTable.getInstance().register(function);
    }
    throw new RuntimeException("Functions not yet supported");
  }

  @Override
  public void loadFile(Env env, URI uri, String name) {
    throw new RuntimeException("Function loading from entire jar tbd");
  }
}
