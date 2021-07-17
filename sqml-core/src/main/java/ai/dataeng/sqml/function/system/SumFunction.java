package ai.dataeng.sqml.function.system;

import ai.dataeng.sqml.function.Signature;
import ai.dataeng.sqml.function.SqlFunction;

public class SumFunction implements SqlFunction {

  @Override
  public Signature getSignature() {
    return null;
  }

  @Override
  public boolean isDeterministic() {
    return false;
  }

  @Override
  public boolean isCalledOnNullInput() {
    return false;
  }

  @Override
  public String getDescription() {
    return null;
  }
}
