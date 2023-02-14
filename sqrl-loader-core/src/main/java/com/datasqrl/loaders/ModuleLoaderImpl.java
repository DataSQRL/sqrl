package com.datasqrl.loaders;

import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystem;
import com.datasqrl.io.DataSystemConfig;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.NamespaceObject;
import lombok.AllArgsConstructor;

import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@AllArgsConstructor
public class ModuleLoaderImpl implements ModuleLoader {
  ResourceResolver resourceResolver;
  StandardLibraryLoader standardLibraryLoader;
  URLObjectLoader objectLoader;

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {

    // Load modules from standard library
    List<NamespaceObject> nsObjects = new ArrayList<>(loadFromStandardLibrary(namePath));

    // Load modules from file system
    if (nsObjects.isEmpty()) {
      nsObjects.addAll(loadFromFileSystem(namePath));
    }

    if (nsObjects.isEmpty()) {
      return Optional.empty();
    }

    return Optional.of(new SqrlDirectoryModule(nsObjects));
  }

  private static boolean isPrintSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
            .equals(PrintDataSystem.SYSTEM_TYPE);
  }

  private List<NamespaceObject> loadFromStandardLibrary(NamePath namePath) {
    if (isPrintSink(namePath)) {
      DataSystemConfig config = PrintDataSystem.DEFAULT_DISCOVERY_CONFIG;
      ErrorCollector errors = ErrorCollector.root();
      DataSystem dataSystem = config.initialize(errors);
      return List.of(new DataSystemNsObject(namePath,dataSystem));
    }

    return standardLibraryLoader.load(namePath);
  }

  private List<NamespaceObject> loadFromFileSystem(NamePath namePath) {

    List<URL> allItems = resourceResolver.loadPath(namePath);
    return allItems.stream()
        .flatMap(url -> objectLoader.load(url, resourceResolver, namePath).stream())
        .collect(Collectors.toList());
  }
}
