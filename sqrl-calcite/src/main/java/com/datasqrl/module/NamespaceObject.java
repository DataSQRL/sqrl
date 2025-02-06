package com.datasqrl.module;

import java.util.Optional;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.validate.ScriptPlanner;

public interface NamespaceObject {

  Name getName();

  boolean apply(ScriptPlanner planner, Optional<String> alias, SqrlFramework framework, ErrorCollector errors);
}
