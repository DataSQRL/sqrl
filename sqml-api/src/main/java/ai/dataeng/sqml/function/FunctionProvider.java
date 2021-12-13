package ai.dataeng.sqml.function;

import ai.dataeng.sqml.function.definition.FunctionDefinition;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.Optional;

public class FunctionProvider {

  public Optional<FunctionDefinition> resolve(QualifiedName name) {
    return Optional.empty();
  }
}
