package com.datasqrl.module;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.canonicalizer.Name;
import com.datasqrl.error.ErrorCollector;

import com.datasqrl.plan.validate.ScriptPlanner;
import java.util.Optional;

public interface NamespaceObject {

  Name getName();

  boolean apply(ScriptPlanner planner, Optional<String> alias, SqrlFramework framework, ErrorCollector errors);
}
