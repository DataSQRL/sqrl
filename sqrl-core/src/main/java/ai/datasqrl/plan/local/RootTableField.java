package ai.datasqrl.plan.local;

import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.ScriptTable;
import lombok.Getter;

public class RootTableField extends Field {

  @Getter
  private final ScriptTable table;

  //Todo: migrate to versioned table
  public RootTableField(ScriptTable table) {
    super(table.getName(),0); //TODO: what should the version be?
    this.table = table;
  }

  @Override
  public String toString() {
    return "RootTableField{" +
        "name=" + name +
        '}';
  }
}