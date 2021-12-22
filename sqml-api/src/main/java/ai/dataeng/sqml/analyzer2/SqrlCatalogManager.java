package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqrlCatalogManager {
  Map<NamePath, SqrlTable> currentTables = new HashMap<>();
  List<SqrlTable> allTables = new ArrayList<>();
  /**
   * Adds a table to the hierarchy
   */
  public void addTable(NamePath namePath, SqrlTable table) {
    currentTables.put(namePath, table);
    allTables.add(table);
  }

  public SqrlTable getCurrentTable(NamePath namePath) {
    return currentTables.get(namePath);
  }

  public Collection<SqrlTable> getCurrentTables() {
    return currentTables.values();
  }

  public Map<NamePath, SqrlTable> getTableMap() {
    return currentTables;
  }
}
