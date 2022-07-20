package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import ai.datasqrl.schema.builder.TableFactory;
import com.google.common.base.Preconditions;
import lombok.Getter;

import java.util.*;

public class Schema {

  private final Map<Name, Table> tables = new HashMap<>();

  //TODO: unpack this
  @Getter
  protected final TableFactory tableFactory = new TableFactory();

  /*
  === Retrieval Methods ===
   */

  protected Table get(Name name) {
    return tables.get(name);
  }

  public void add(Table table) {
    Preconditions.checkArgument(table.getPath().getLength()==1,"Only add root tables to schema");
    tables.put(table.getName(),table);
  }

  public Optional<Table> getTable(Name name) {
    return Optional.ofNullable(get(name));
  }

  public Table walkTable(NamePath tablePath) {
    if (tablePath.getLength() == 1) {
      return get(tablePath.getFirst());
    } else {
      return get(tablePath.getFirst())
              .walkTable(tablePath.popFirst()).get();
    }
  }

  public List<Table> allTables() {
    List<Table> tableList = new ArrayList<>();
    for (Table t : tables.values()) addTableAndChildren(t,tableList);
    return tableList;
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
