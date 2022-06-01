package ai.datasqrl.plan.calcite;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelTrait;
import org.apache.calcite.rel.RelNode;

public class SqrlConvention extends Convention.Impl {

  public SqrlConvention(String name, Class<? extends RelNode> relClass) {
    super(name, relClass);
  }

  @Override
  public boolean satisfies(RelTrait trait) {
    return true;
  }

}
