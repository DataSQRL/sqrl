package com.datasqrl.loaders;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.module.NamespaceObject;
import lombok.Value;

@Value
public class DataSystemNsObject implements NamespaceObject {
  NamePath path;
  DataSystemDiscovery discovery;

  public Name getName() {
    return path.getLast();
  }
}
