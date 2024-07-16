package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.TableConfigLoader;
import com.datasqrl.engine.log.LogManager;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.datasqrl.module.resolver.ResourceResolver;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor(onConstructor_=@Inject)
public class ModuleLoaderImpl implements ModuleLoader {

  final StandardLibraryLoader standardLibraryLoader = new StandardLibraryLoader();
  private final ResourceResolver resourceResolver;
  private final ErrorCollector errors;
  private final CalciteTableFactory tableFactory;
  private final TableConfigLoader tableConfigFactory;
  private final PackageJson sqrlConfig;
  private final LogManager logManager;

  // Required to reduce the cost of script imports
  private final Map<NamePath, SqrlModule> cache = new HashMap<>();
  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    if (cache.containsKey(namePath)) {
      return Optional.of(cache.get(namePath));
    }

    Optional<SqrlModule> module = getModuleOpt(namePath);
    module.ifPresent(sqrlModule -> cache.put(namePath, sqrlModule));

    return module;
  }

  public Optional<SqrlModule> getModuleOpt(NamePath namePath) {
    // Load modules from standard library
    Optional<SqrlModule> module = loadFromStandardLibrary(namePath);
    if (module.isPresent()) {
      return module;
    }

    // Load modules from file system
    return loadFromFileSystem(namePath);
  }

  private Optional<SqrlModule> loadFromStandardLibrary(NamePath namePath) {
    List<NamespaceObject> lib = standardLibraryLoader.load(namePath);
    if (lib.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new SqrlDirectoryModule(lib));
  }

  private Optional<SqrlModule> loadFromFileSystem(NamePath namePath) {
    return new ObjectLoaderImpl(resourceResolver, errors, tableFactory, this, tableConfigFactory, sqrlConfig, logManager)
        .load(namePath);
  }

  @Override
  public String toString() {
    return new ObjectLoaderImpl(resourceResolver, errors, tableFactory, this, tableConfigFactory, sqrlConfig, logManager).toString();
  }

}
