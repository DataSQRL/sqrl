package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
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

  public abstract Name getId();

  public Table getTable() {
    return null;
  }
}
