package org.apache.calcite.tools;

import ai.dataeng.sqml.catalog.Namespace;
import ai.dataeng.sqml.planner.Planner;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Optional;
import lombok.Value;
import org.apache.calcite.tools.SqrlRelBuilder;

@Value
public class SqrlRelBuilderFactory {
  Planner planner;
  Optional<NamePath> context;
  Namespace namespace;

  public SqrlRelBuilder create() {
    return planner.getRelBuilder(context, namespace);
  }
}