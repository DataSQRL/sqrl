package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * It is not possible to define new tables inside a dataset (only in the root scope of the script)
 * so we don't have to consider shadowing of tables within a dataset.
 */
public class Dataset implements DatasetOrTable {

  public final Name name;
  public final List<Table> tables;

  public Dataset(Name name) {
    this(name, new ArrayList<>());
  }

  public Dataset(Name name, List<Table> tables) {
    this.name = name;
    this.tables = tables;
  }

  @Override
  public Name getName() {
    return name;
  }

  public Optional<Table> get(Name name) {
    for (int i = tables.size() - 1; i >= 0; i--) {
      Table table = tables.get(i);
      if (table.getName().equals(name)) {
        return Optional.of(table);
      }
    }
    return Optional.empty();
  }

  public Optional<Table> walk(NamePath namePath) {
    Optional<Table> table = get(namePath.getFirst());
    if (table.isEmpty()) {
      return table;
    }
    if (namePath.getLength() == 1) {
      return table;
    }
    return table.get().walk(namePath.popFirst());
  }

  public void merge(Dataset dataset) {
    this.tables.addAll(dataset.tables);
  }
}
