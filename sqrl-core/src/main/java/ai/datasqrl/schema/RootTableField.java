package ai.datasqrl.schema;

import lombok.Getter;

public class RootTableField extends Field {

  @Getter
  private final Table table;

  //Todo: migrate to versioned table
  public RootTableField(Table table) {
    super(table.getName());
    this.table = table;
  }

  @Override
  public String toString() {
    return "RootTableField{" +
        "name=" + name +
        '}';
  }
}