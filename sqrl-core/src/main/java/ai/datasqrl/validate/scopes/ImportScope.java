package ai.datasqrl.validate.scopes;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.validate.Namespace;
import ai.datasqrl.validate.imports.ImportManager.SourceTableImport;
import java.util.Optional;
import lombok.Value;

@Value
public class ImportScope implements ValidatorScope {
  NamePath name;
  Optional<Name> alias;
  SourceTableImport sourceTableImport;

  @Override
  public Namespace getNamespace() {
    return null;
  }
}
