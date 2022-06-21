package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.NamePath;

import java.util.ArrayList;
import java.util.List;

public class Schema extends ShadowingContainer<Table> {

  public Table walkTable(NamePath tablePath) {
    if (tablePath.getLength() == 1) {
      return this.getVisibleByName(tablePath.getFirst()).get();
    } else {
      return this.getVisibleByName(tablePath.getFirst()).get()
          .walk(tablePath.popFirst()).get();
    }
  }

  public List<Table> allTables() {
    List<Table> tables = new ArrayList<>();
    for (Table t : this) addTableAndChildren(t,tables);
    return tables;
  }

  private void addTableAndChildren(Table t, List<Table> tables) {
    tables.add(t);
    for (Table child : t.getChildren()) {
      addTableAndChildren(child, tables);
    }
  }

  public String toString() {
    StringBuilder s = new StringBuilder();
    for (Table t : allTables()) {
      s.append(t).append("\n");
    }
    return s.toString();
  }

}
