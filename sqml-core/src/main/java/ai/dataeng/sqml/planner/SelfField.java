package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.VersionedName;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class SelfField extends Field {

  public SelfField(Table table) {
    super(table.getName(), table);
  }

  public VersionedName getId() {
    return VersionedName.of(Name.system("_"), table.getVersion());
  }

  @Override
  public int getVersion() {
    return table.getVersion();
  }
}
