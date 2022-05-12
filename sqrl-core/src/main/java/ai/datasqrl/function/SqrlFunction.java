package ai.datasqrl.function;

public interface SqrlFunction {

  boolean isAggregate();
  boolean requiresOver();
}
