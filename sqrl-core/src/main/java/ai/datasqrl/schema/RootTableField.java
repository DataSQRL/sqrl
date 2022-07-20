package ai.datasqrl.schema;

import lombok.Getter;

public class RootTableField extends Field {

  @Getter
  private final VarTable table;

  //Todo: migrate to versioned table
  public RootTableField(VarTable table) {
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