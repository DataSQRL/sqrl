package com.datasqrl.module;

import java.util.List;
import java.util.Optional;

import com.datasqrl.canonicalizer.Name;

public interface SqrlModule {

  Optional<NamespaceObject> getNamespaceObject(Name name);

  List<NamespaceObject> getNamespaceObjects();
}
