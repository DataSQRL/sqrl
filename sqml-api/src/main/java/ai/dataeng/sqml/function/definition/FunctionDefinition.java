package ai.dataeng.sqml.function.definition;

import ai.dataeng.sqml.function.definition.inference.TypeInference;

public interface FunctionDefinition {
  FunctionKind getKind();

  TypeInference getTypeInference();

  default boolean overWindowOnly() {
    return false;
  }

  default boolean isDeterministic() {
    return true;
  }
}
