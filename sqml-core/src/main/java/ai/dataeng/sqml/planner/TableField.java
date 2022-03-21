package ai.dataeng.sqml.planner;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class TableField extends Field {

  public TableField(Table table) {
    super(table.getName(), table);
  }

  public String getId() {
    return name.getCanonical() + SchemaImpl.ID_DELIMITER + Integer.toHexString(table.getVersion());
  }

  @Override
  public int getVersion() {
    return table.getVersion();
  }
}
