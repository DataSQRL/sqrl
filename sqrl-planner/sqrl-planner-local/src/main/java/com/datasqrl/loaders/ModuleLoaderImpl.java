package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.impl.print.PrintDataSystemFactory;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AllArgsConstructor(onConstructor_=@Inject)
@Singleton //todo shouldn't be singleton
public class ModuleLoaderImpl implements ModuleLoader {
  final StandardLibraryLoader standardLibraryLoader = new StandardLibraryLoader();
  ObjectLoader objectLoader;
  private final Map<NamePath, SqrlModule> modules = new HashMap<>();

  @Override
  public Optional<SqrlModule> getModule(NamePath namePath) {
    SqrlModule module;
    if ((module = modules.get(namePath))!=null) {
      return Optional.of(module);
    }
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

  @Override
  public void add(NamePath namePath, SqrlModule module) {
    modules.put(namePath, module);
  }

  private static boolean isPrintSink(NamePath namePath) {
    return namePath.size() == 1 && namePath.getLast().getCanonical()
            .equals(PrintDataSystemFactory.SYSTEM_NAME);
  }

  private List<NamespaceObject> loadFromStandardLibrary(NamePath namePath) {
    if (isPrintSink(namePath)) {
      DataSystemDiscovery printSinks = PrintDataSystemFactory.getDefaultDiscovery();
      return List.of(new DataSystemNsObject(namePath, printSinks));
    }

    return standardLibraryLoader.load(namePath);
  }

  private List<NamespaceObject> loadFromFileSystem(NamePath namePath) {
    return objectLoader.load(namePath);

  }

  @Override
  public String toString() {
    return objectLoader.toString();
  }

}
