package com.datasqrl.loaders;

import com.datasqrl.io.DataSystem;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.module.NamespaceObject;
import lombok.Value;

@Value
public class DataSystemNsObject implements NamespaceObject {
  NamePath path;
  DataSystem table;

  public Name getName() {
    return path.getLast();
  }
}
