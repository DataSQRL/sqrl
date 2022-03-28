package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.VersionedName;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class TableField extends Field {

  public TableField(Table table) {
    super(table.getName(), table);
  }

  public VersionedName getId() {
    return VersionedName.of(name, table.getVersion());
  }

  @Override
  public int getVersion() {
    return table.getVersion();
  }
}
