package ai.datasqrl.function;

import ai.datasqrl.parse.tree.name.Name;

/**
 * Sqrl defined function should implement this function provide extra metadata
 */
public interface SqrlAwareFunction {
  Name getSqrlName();
  boolean isAggregate();
  boolean requiresOver();
  boolean isDeterministic();
  boolean isTimestampPreserving();
}
