package ai.datasqrl.physical.database.ddl;

import lombok.AllArgsConstructor;

@AllArgsConstructor
public class DropTableDDL implements SqlDDLStatement {
  String name;
  @Override
  public String toSql() {
    return "DROP TABLE IF EXISTS " + name + ";";
  }
}
