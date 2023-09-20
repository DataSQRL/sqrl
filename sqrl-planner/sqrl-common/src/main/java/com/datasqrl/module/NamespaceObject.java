package com.datasqrl.module;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;

import java.util.Optional;

public interface NamespaceObject {

  Name getName();

  boolean apply(Optional<String> alias, SqrlFramework framework, ErrorCollector errors);
}
