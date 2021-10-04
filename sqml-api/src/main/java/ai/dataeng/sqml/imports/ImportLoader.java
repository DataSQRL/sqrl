package ai.dataeng.sqml.imports;

import ai.dataeng.sqml.function.SqmlFunction;
import com.google.common.collect.ImmutableSet;
import com.google.common.reflect.ClassPath;
import com.google.common.reflect.ClassPath.ClassInfo;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.SneakyThrows;

public class ImportLoader {
  private Map<String, ImportObject> importObjects = new HashMap<>();

  public void register(String name, ImportObject object) {
    importObjects.put(name, object);
  }

  public List<ImportObject> load(String value) {
    List<ImportObject> local = loadLocal(value);
    if (local.isEmpty()) {
      return loadClasspath(value);
    }
    return local;
  }

  @SneakyThrows
  private List<ImportObject> loadClasspath(String value) {
    int idx = value.lastIndexOf(".");
    String pkg = value.substring(0, idx);
    String functionName = value.substring(idx + 1);
    if (functionName.equalsIgnoreCase("*")) {
      return resolvePackage(pkg);
    }

    return List.of(resolveClass(Class.forName(value)));
  }

  @SneakyThrows
  private List<ImportObject> resolvePackage(String pkg) {
    ImmutableSet<ClassInfo> classInfos = ClassPath.from(getClass().getClassLoader())
        .getTopLevelClasses(pkg);

    return classInfos.stream()
        .map(c->resolveClass(c.load()))
        .collect(Collectors.toList());
  }

  @SneakyThrows
  private ImportObject resolveClass(Class<?> clazz) {
    Object o = clazz.getDeclaredConstructor().newInstance();
    if (!(o instanceof SqmlFunction)) {
      throw new RuntimeException(String.format("Resolved import is not a function %s", clazz.getName()));
    }
    return new FunctionImportObject(
        (SqmlFunction) clazz.getDeclaredConstructor().newInstance(),
        clazz.getTypeName(),
        clazz.getName());
  }

  public List<ImportObject> loadLocal(String value) {
    if (value.endsWith("*")) {
      String newValue = value.substring(0, value.lastIndexOf("."));

      return importObjects.entrySet().stream()
          .filter(e->matchesPrefix(e.getKey(), newValue))
          .map(e->e.getValue())
          .collect(Collectors.toList());
    }

    return importObjects.entrySet().stream()
        .filter(e->matches(e.getKey(), value))
        .map(e->e.getValue())
        .collect(Collectors.toList());
  }

  private boolean matches(String key, String value) {
    return key.equalsIgnoreCase(value);
  }

  private boolean matchesPrefix(String key, String value) {
    String newKey = key.substring(0, key.lastIndexOf("."));
    return newKey.equalsIgnoreCase(value);
  }
}
