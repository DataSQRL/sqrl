package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.VersionedName;
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
    return new VersionedName(this.getName().getCanonical(), this.getName().getDisplay(),
        getVersion());
  }

  public Table getTable() {
    return null;
  }

}
