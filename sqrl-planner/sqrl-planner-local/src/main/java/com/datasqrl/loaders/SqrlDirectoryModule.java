package com.datasqrl.loaders;

import com.datasqrl.module.SqrlModule;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.module.NamespaceObject;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;

public class SqrlDirectoryModule implements SqrlModule {
  List<NamespaceObject> nsObjects;

  public SqrlDirectoryModule(List<NamespaceObject> nsObjects) {
    if (nsObjects instanceof ArrayList) //check for mutable lists to sort (for consistent tests and behavior)
      nsObjects.sort(Comparator.comparing(NamespaceObject::getName));
    this.nsObjects = nsObjects;
  }

  @Override
  public Optional<NamespaceObject> getNamespaceObject(Name name) {
    return nsObjects.stream()
        .filter(f->f.getName().equals(name))
        .findAny();
  }

  @Override
  public List<NamespaceObject> getNamespaceObjects() {
    return nsObjects;
  }
}
