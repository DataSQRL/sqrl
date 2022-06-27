package ai.datasqrl.function;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.Optional;

public interface FunctionMetadataProvider {
  Optional<SqrlAwareFunction> lookup(NamePath path);
}
