package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.function.PythonNamespaceObject;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.functions.python.PythonFunction;
import org.apache.flink.table.functions.python.utils.PythonFunctionUtils;

public class PythonSqrlModule implements SqrlModule {

  private final Path path;
  private final NamePath directory;
  private final PackageJson sqrlConfig;

  public PythonSqrlModule(Path path, NamePath directory, PackageJson sqrlConfig) {
    this.path = path;
    this.directory = directory;
    this.sqrlConfig = sqrlConfig;
  }

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    Map<String, Object> flinkConfig = Optional.ofNullable(sqrlConfig.toMap().get("values"))
        .map(e->(Map)e)
        .flatMap(e->Optional.ofNullable(e.get("flink-config")))
        .map(e->(Map)e)
        .get();

    String execKey = "python.executable";
    String execClientKey = "python.client.executable";
    String exec = (String)flinkConfig.get(execKey);
    String execClient = (String)flinkConfig.get(execClientKey);

    if (execClient == null || exec == null) {
      throw new RuntimeException("python.client.executable and python.executable must be set");
    }

    Configuration configuration = new Configuration();
    configuration.setString(execKey, exec);
    configuration.setString(execClientKey, execClient);
    configuration.setString("python.files", path.toFile().getParent().toString());

    PythonFunction pythonFunction = PythonFunctionUtils.getPythonFunction(
        path.getFileName().toString().substring(0, path.getFileName().toString().length()-3)
            + "." + name.getDisplay(), configuration,
        this.getClass().getClassLoader());

    return Optional.of(new PythonNamespaceObject(pythonFunction, name, path.toFile().getParent().toString(),
        directory));
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    throw new RuntimeException("Cannot enumerate functions in python files, use fully qualified names");
  }
}
