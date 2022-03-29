package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.VersionedName;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString(callSuper = true)
public class SelfField extends Field {

  private final Table table;

  public SelfField(Table table) {
    super(table.getName());
    this.table = table;
  }

  public VersionedName getId() {
    return VersionedName.of(Name.system("_"), table.getVersion());
  }

  @Override
  public int getVersion() {
    return table.getVersion();
  }
}
