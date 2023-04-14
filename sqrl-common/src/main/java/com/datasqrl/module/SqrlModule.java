package com.datasqrl.module;

import com.datasqrl.canonicalizer.Name;
import java.util.List;
import java.util.Optional;

public interface SqrlModule {

  Optional<NamespaceObject> getNamespaceObject(Name name);

  List<NamespaceObject> getNamespaceObjects();
}
