package com.datasqrl.loaders;

import com.datasqrl.name.Name;
import com.datasqrl.plan.local.generate.NamespaceObject;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import lombok.AllArgsConstructor;

public class SqrlDirectoryModule implements SqrlModule {
  List<NamespaceObject> nsObjects;

  public SqrlDirectoryModule(List<NamespaceObject> nsObjects) {
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
