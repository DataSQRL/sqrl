package ai.dataeng.sqml.planner;

import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.VersionedName;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

@Getter
@ToString
@Setter
public abstract class Field implements ShadowingContainer.Nameable {

  public Name name;
  public Table table;

  protected Field(Name name, Table table) {
    this.name = name;
    this.table = table;
  }

  public abstract VersionedName getId();
}
