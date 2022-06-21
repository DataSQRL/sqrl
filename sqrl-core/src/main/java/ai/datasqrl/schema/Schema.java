package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.NamePath;

public class Schema extends ShadowingContainer<Table> {

  public Table walkTable(NamePath tablePath) {
    if (tablePath.getLength() == 1) {
      return this.getVisibleByName(tablePath.getFirst()).get();
    } else {
      return this.getVisibleByName(tablePath.getFirst()).get()
          .walk(tablePath.popFirst()).get();
    }
  }
}
