package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.io.impl.print.PrintDataSystem;
import com.datasqrl.io.impl.print.PrintDataSystemFactory;
import com.datasqrl.module.NamespaceObject;
import com.datasqrl.module.SqrlModule;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class ModuleLoaderImpl implements ModuleLoader {
  final StandardLibraryLoader standardLibraryLoader = new StandardLibraryLoader();
  ObjectLoader objectLoader;

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
