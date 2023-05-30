package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;

import java.net.URI;
import java.util.List;

public interface ObjectLoader {

  List<NamespaceObject> load(NamePath namePath);

  ObjectLoaderMetadata getMetadata(NamePath namePath);
}
