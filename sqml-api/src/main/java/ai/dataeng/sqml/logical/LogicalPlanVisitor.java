package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.type.SqmlTypeVisitor;

public abstract class LogicalPlanVisitor<R, C> extends SqmlTypeVisitor<R, C> {
  public abstract R visit(LogicalPlan logicalPlan, C context);

}
