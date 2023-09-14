package com.datasqrl.plan.local.generate;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.plan.OptimizationStage;
import com.datasqrl.plan.RelStageRunner;
import com.datasqrl.plan.table.CalciteTableFactory;
import com.google.inject.Inject;
import lombok.Getter;
import org.apache.calcite.jdbc.SqrlSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.tools.RelBuilder;

/**
 * A facade to the calcite planner
 */
@Getter
@Deprecated
public class SqrlQueryPlanner {
  private final SqrlFramework framework;
  private final CalciteTableFactory tableFactory;

  @Inject
  public SqrlQueryPlanner(
      SqrlFramework framework,
      CalciteTableFactory tableFactory) {
    this.framework = framework;
    this.tableFactory = tableFactory;
  }

  public RelBuilder createRelBuilder() {
    return framework.getQueryPlanner().getRelBuilder();
  }

  public SqrlSchema getSchema() {
    return framework.getSchema();
  }
}
