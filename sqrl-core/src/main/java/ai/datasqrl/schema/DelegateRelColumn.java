package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import lombok.NonNull;

public class DelegateRelColumn extends Column {

  protected DelegateRelColumn(@NonNull Name name, int version) {
    super(name, version, !name.isHidden());
  }
}
