package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.resolver.ResourceResolver;
import com.google.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class ModuleLoaderImpl implements ModuleLoader {

  final ClasspathFunctionLoader classpathFunctionLoader = new ClasspathFunctionLoader();
  private final ResourceResolver resourceResolver;
  private final ErrorCollector errors;

  // Required to reduce the cost of script imports
  private final Map<NamePath, SqrlModule> cache = new HashMap<>();
  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    if (cache.containsKey(namePath)) {
      return Optional.of(cache.get(namePath));
    }

    var module = getModuleOpt(namePath);
    module.ifPresent(sqrlModule -> cache.put(namePath, sqrlModule));

    return module;
  }

  public Optional<SqrlModule> getModuleOpt(NamePath namePath) {
    // Load modules from file system first
    var module = loadFromFileSystem(namePath);
    if (module.isEmpty()) { //if it's not local, try to load it from classpath
      module = loadFunctionsFromClasspath(namePath);
    }
    return module;
  }

  private Optional<SqrlModule> loadFunctionsFromClasspath(NamePath namePath) {
    var lib = classpathFunctionLoader.load(namePath);
    if (lib.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new SqrlDirectoryModule(lib));
  }

  private Optional<SqrlModule> loadFromFileSystem(NamePath namePath) {
    return new ObjectLoaderImpl(resourceResolver, errors)
        .load(namePath);
  }

  @Override
  public String toString() {
    return new ObjectLoaderImpl(resourceResolver, errors).toString();
  }

}
