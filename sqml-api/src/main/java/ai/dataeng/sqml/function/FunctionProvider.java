package ai.dataeng.sqml.function;

import ai.dataeng.sqml.function.definition.BuiltInFunctionDefinitions;
import ai.dataeng.sqml.function.definition.FunctionDefinition;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.Optional;

public class FunctionProvider {

  public Optional<FunctionDefinition> resolve(QualifiedName name) {
    if (name.toString().equalsIgnoreCase("count")) {
      return Optional.of(BuiltInFunctionDefinitions.COUNT);
    }

    return Optional.empty();
  }
}
