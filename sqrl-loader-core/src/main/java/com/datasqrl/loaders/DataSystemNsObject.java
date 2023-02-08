package com.datasqrl.loaders;

import com.datasqrl.io.DataSystem;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.name.Name;
import com.datasqrl.name.NamePath;
import com.datasqrl.plan.local.generate.NamespaceObject;
import com.datasqrl.plan.local.generate.TableNamespaceObject;
import lombok.Value;

@Value
public class DataSystemNsObject implements NamespaceObject {
  NamePath path;
  DataSystem table;

  public Name getName() {
    return path.getLast();
  }
}
