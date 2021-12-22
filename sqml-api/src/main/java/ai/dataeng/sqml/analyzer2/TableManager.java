package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.tree.name.NamePath;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.Value;

@Getter
public class TableManager {
  private Map<NamePath, MaterializeTable> lastSeenFlinkTable = new HashMap<>();
  private List<MaterializeTable> views = new ArrayList<>();

  private Map<NamePath, MaterializeTable> tables = new HashMap<>();
  private Map<SqrlTable, String> curName = new HashMap<>();

  int viewNum = 0;
  boolean readMode = false;

  public void setTable(NamePath name, SqrlTable entity, String query) {
    String sqlFriendlyName = name.toString().replaceAll("\\.", "_");
    String flinkName = sqlFriendlyName + "_flink";
    String viewName = sqlFriendlyName + "_" + (viewNum);

    if (readMode) {
      ++viewNum;
      query = "CREATE VIEW " + viewName + " AS " + query;
    }

    MaterializeTable table = new MaterializeTable(flinkName, viewName, name, entity, query, readMode);
    tables.put(name, table);

    if (!readMode) {
      curName.put(entity, flinkName);
      lastSeenFlinkTable.put(name, table);
    } else {
      curName.put(entity, viewName);
      views.add(table);
    }
  }

  public String getCurrentName(SqrlTable entity) {
    return curName.get(entity);
  }

  public SqrlTable getTable(NamePath name) {
    return tables.get(name).entity;
  }

  public MaterializeTable getMatTable(NamePath name) {
    return tables.get(name);
  }

  public void setReadMode() {
    if (readMode) {
      return;
    }

    this.readMode = true;
  }

  public Collection<MaterializeTable> getFinalFlinkTables() {
    return lastSeenFlinkTable.values();
  }

  @Value
  class MaterializeTable {
    String flinkName;
    String viewName;
    NamePath namePath;
    SqrlTable entity;
    String query;
    boolean isView;
  }
}
