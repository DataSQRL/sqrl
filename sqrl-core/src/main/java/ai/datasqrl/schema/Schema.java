package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.builder.TableFactory;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.*;

public class Schema {

  private final Map<Name, VarTable> tables = new HashMap<>();

  //TODO: unpack this
  @Getter
  protected final TableFactory tableFactory = new TableFactory();

  /*
  === Retrieval Methods ===
   */

  protected VarTable get(Name name) {
    return tables.get(name);
  }

  public void add(VarTable table) {
    Preconditions.checkArgument(table.getPath().size()==1,"Only add root tables to schema");
    tables.put(table.getName(),table);
  }

  public Optional<VarTable> getTable(Name name) {
    return Optional.ofNullable(get(name));
  }

  public VarTable walkTable(NamePath tablePath) {
    if (tablePath.size() == 1) {
      return get(tablePath.getFirst());
    } else {
      return get(tablePath.getFirst())
              .walkTable(tablePath.popFirst()).get();
    }
  }

  public List<VarTable> allTables() {
    List<VarTable> tableList = new ArrayList<>();
    for (VarTable t : tables.values()) addTableAndChildren(t,tableList);
    return tableList;
  }

  private void addTableAndChildren(VarTable t, List<VarTable> tables) {
    tables.add(t);
    for (VarTable child : t.getChildren()) {
      addTableAndChildren(child, tables);
    }
  }

  public String toString() {
    StringBuilder s = new StringBuilder();
    for (VarTable t : allTables()) {
      s.append(t).append("\n");
    }
    return s.toString();
  }



}
