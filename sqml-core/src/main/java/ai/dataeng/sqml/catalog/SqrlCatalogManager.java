package ai.dataeng.sqml.catalog;

import ai.dataeng.sqml.tree.name.NamePath;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class SqrlCatalogManager {
  Map<NamePath, SqrlTable2> currentTables = new HashMap<>();
  Map<NamePath, SqrlTable2> versionedTables = new HashMap<>();

  public void addTable(SqrlTable2 table) {
    currentTables.put(table.getName(), table);
    versionedTables.put(table.getVersionedName(), table);
  }

  public SqrlTable2 getCurrentTable(NamePath namePath) {
    return currentTables.get(namePath);
  }

  public Collection<SqrlTable2> getCurrentTables() {
    return currentTables.values();
  }

  public Map<NamePath, SqrlTable2> getTableMap() {
    return currentTables;
  }
}
