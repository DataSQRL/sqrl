package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.VersionedName;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class TableField extends Field {

  private final Table table;

  public TableField(Table table) {
    super(table.getName());
    this.table = table;
  }

  public VersionedName getId() {
    return VersionedName.of(name, table.getVersion());
  }

  @Override
  public int getVersion() {
    return table.getVersion();
  }
}
