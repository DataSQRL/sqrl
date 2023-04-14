package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;

import java.util.List;

public interface ObjectLoader {

  List<NamespaceObject> load(NamePath namePath);
}
