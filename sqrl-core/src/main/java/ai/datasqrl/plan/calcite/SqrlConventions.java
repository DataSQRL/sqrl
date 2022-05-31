package ai.datasqrl.plan.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.rel.RelNode;

//todo
public class SqrlConventions {
  public static Convention LOGICAL = new SqrlConvention("Logical", RelNode.class);
}
