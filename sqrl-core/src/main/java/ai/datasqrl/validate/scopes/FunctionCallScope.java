package ai.datasqrl.validate.scopes;

import ai.datasqrl.function.SqrlFunction;
import lombok.Value;

/**
 *
 */
@Value
public class FunctionCallScope extends ExpressionScope {

  SqrlFunction resolvedFunction;
  public boolean isAggregating;
}
