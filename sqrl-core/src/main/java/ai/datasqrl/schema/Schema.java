package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.NamePath;
import java.util.Optional;

public class Schema extends ShadowingContainer<Table> {

  public Table walkTable(NamePath tablePath) {

    if (tablePath.getLength() == 1) {
      return this.getByName(tablePath.getFirst()).get();
    } else {
      return this.getByName(tablePath.getFirst()).get()
          .walk(tablePath.popFirst()).get();
    }
  }
}
