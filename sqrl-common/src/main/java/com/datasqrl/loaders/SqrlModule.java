package com.datasqrl.loaders;

import com.datasqrl.name.Name;
import com.datasqrl.plan.local.generate.NamespaceObject;
import java.util.List;
import java.util.Optional;

public interface SqrlModule {

  Optional<NamespaceObject> getNamespaceObject(Name name);

  List<NamespaceObject> getNamespaceObjects();
}
