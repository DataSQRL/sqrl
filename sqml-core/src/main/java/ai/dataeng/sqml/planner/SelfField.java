package ai.dataeng.sqml.planner;

import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class SelfField extends Field {

  public SelfField(Table table) {
    super(table.getName(), table);
  }

  public String getId() {
    return table.getId();
  }

  @Override
  public int getVersion() {
    return table.getVersion();
  }
}
