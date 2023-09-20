package com.datasqrl.loaders;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.canonicalizer.NamePath;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.io.DataSystemDiscovery;
import com.datasqrl.module.NamespaceObject;
import lombok.AllArgsConstructor;
import lombok.Getter;

import java.util.Optional;

@AllArgsConstructor
@Getter
public class DataSystemNsObject implements NamespaceObject {
  NamePath path;
  DataSystemDiscovery discovery;

  public Name getName() {
    return path.getLast();
  }

  @Override
  public boolean apply(Optional<String> name, SqrlFramework framework, ErrorCollector errors) {
    throw new RuntimeException("Cannot import data system");
  }
}
