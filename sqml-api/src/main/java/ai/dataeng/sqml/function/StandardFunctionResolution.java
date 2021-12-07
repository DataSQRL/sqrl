package ai.dataeng.sqml.function;

import ai.dataeng.sqml.OperatorType;
import ai.dataeng.sqml.schema2.Type;

public class StandardFunctionResolution {

  public FunctionHandle comparisonFunction(OperatorType newOperator, Type type, Type type1) {
    return null;
  }

  public FunctionHandle notFunction() {
    return null;
  }

  public boolean isComparisonFunction(FunctionHandle functionHandle) {
    return false;
  }
}
