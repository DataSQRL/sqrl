package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.parser.operator.ShadowingContainer;
import ai.dataeng.sqml.tree.Node;
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

  protected Field(Name name) {
    this.name = name;
  }

  public VersionedName getId() {
    return new VersionedName(this.getName().getCanonical(), this.getName().getDisplay(), getVersion());
  }

  public Table getTable() {
    return null;
  }

}
