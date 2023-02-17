package com.datasqrl.loaders;

import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.NamespaceObject;

import java.util.List;

public interface ObjectLoader {

  List<NamespaceObject> load(NamePath namePath);
}
