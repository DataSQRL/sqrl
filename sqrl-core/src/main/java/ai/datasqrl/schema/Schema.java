package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.base.Preconditions;

import java.util.*;

public class Schema {

  private final Map<Name, ScriptTable> tables = new HashMap<>();

  /*
  === Retrieval Methods ===
   */

  protected ScriptTable get(Name name) {
    return tables.get(name);
  }

  public void add(ScriptTable table) {
    Preconditions.checkArgument(table.getPath().size()==1,"Only add root tables to schema");
    tables.put(table.getName(),table);
  }

  public Optional<ScriptTable> getTable(Name name) {
    return Optional.ofNullable(get(name));
  }

  public ScriptTable walkTable(NamePath tablePath) {
    if (tablePath.size() == 1) {
      return get(tablePath.getFirst());
    } else {
      return get(tablePath.getFirst())
              .walkTable(tablePath.popFirst()).get();
    }
  }

  public List<ScriptTable> allTables() {
    List<ScriptTable> tableList = new ArrayList<>();
    for (ScriptTable t : tables.values()) addTableAndChildren(t,tableList);
    return tableList;
  }

  private void addTableAndChildren(ScriptTable t, List<ScriptTable> tables) {
    tables.add(t);
    for (ScriptTable child : t.getChildren()) {
      addTableAndChildren(child, tables);
    }
  }

  public String toString() {
    StringBuilder s = new StringBuilder();
    for (ScriptTable t : allTables()) {
      s.append(t).append("\n");
    }
    return s.toString();
  }



}
