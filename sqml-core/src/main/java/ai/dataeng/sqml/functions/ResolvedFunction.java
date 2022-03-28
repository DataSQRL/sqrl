package ai.dataeng.sqml.functions;

import lombok.Value;

/**
 * A fully qualified function
 */
@Value
public class ResolvedFunction {
  public String namespace;
  public SqrlFunction function;
}
