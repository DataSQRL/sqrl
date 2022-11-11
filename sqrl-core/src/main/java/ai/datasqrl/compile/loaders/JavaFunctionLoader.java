package ai.datasqrl.compile.loaders;

import ai.datasqrl.function.builtin.time.StdTimeLibraryImpl.FlinkFnc;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.plan.local.generate.Resolve.Env;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.validation.constraints.NotNull;
import org.apache.flink.calcite.shaded.com.google.common.base.Preconditions;
import org.apache.flink.table.functions.UserDefinedFunction;

/**
 * All jars are loaded on the class path and resolved with java's ServiceLoader.
 *
 */
public class JavaFunctionLoader extends AbstractLoader {
  public static final String FILE_SUFFIX = ".function.json";
  private static final Pattern CONFIG_FILE_PATTERN = Pattern.compile("(.*)\\.function\\.json$");

  @Override
  public Optional<String> handles(Path file) {
    Matcher matcher = CONFIG_FILE_PATTERN.matcher(file.getFileName().toString());
    if (matcher.find()) {
      return Optional.of(matcher.group(1));
    }
    return Optional.empty();
  }

  @Override
  public boolean load(Env env, NamePath fullPath, Optional<Name> alias) {
    NamePath basePath = fullPath.subList(0,fullPath.size()-1);

    Path baseDir = namepath2Path(env.getPackagePath(), basePath);

    Path path = baseDir.resolve(fullPath.getLast() + FILE_SUFFIX);
    Preconditions.checkState(path.toFile().isFile(), "Could not find function %s %s", fullPath,
        path);

    FunctionJson fnc = mapJsonFile(path, FunctionJson.class);
    try {
      Class<?> clazz = Class.forName(fnc.classPath);
      env.getResolvedFunctions().add(
          new FlinkFnc(alias.map(Name::getCanonical).orElse(clazz.getSimpleName()),
              (UserDefinedFunction) clazz.getDeclaredConstructor().newInstance()
          ));
    } catch (ClassNotFoundException | NoSuchMethodException | InstantiationException |
             IllegalAccessException | InvocationTargetException e) {
      throw new RuntimeException(String.format("Could not find or load class name: %s", fnc.classPath), e);
    }

    return true;
  }

  public static class FunctionJson {
    @NotNull
    public String classPath;
  }
}
