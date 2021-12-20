package ai.dataeng.sqml.analyzer2;

import ai.dataeng.sqml.logical4.LogicalPlan;
import ai.dataeng.sqml.logical4.LogicalPlan.Table;
import ai.dataeng.sqml.tree.name.Name;

public class NameTranslator {

  public void registerTable(LogicalPlan.Table table, String jdbcTable) {

  }

  public void registerField(LogicalPlan.Field field, Name name) {

  }

  public String getGraphqlName(Table tbl) {
    return tbl.getName().getCanonical();
  }
  public String getGraphqlTypeName(Table tbl) {
    return tbl.getName().getDisplay();
  }
}
