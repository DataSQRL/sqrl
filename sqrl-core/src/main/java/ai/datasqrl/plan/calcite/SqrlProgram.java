package ai.datasqrl.plan.calcite;

import org.apache.calcite.rel.RelNode;

public abstract class SqrlProgram {

  public abstract RelNode apply(RelNode relNode);
}
