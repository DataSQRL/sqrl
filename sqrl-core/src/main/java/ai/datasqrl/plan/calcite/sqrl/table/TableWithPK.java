package ai.datasqrl.plan.calcite.sqrl.table;

import java.util.List;
public  interface TableWithPK {
  String getNameId();
  public List<String> getPrimaryKeys();
}