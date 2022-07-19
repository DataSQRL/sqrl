package ai.datasqrl.schema;

import ai.datasqrl.parse.tree.name.Name;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
public abstract class Field {

  protected final Name name;

  protected Field(Name name) {
    this.name = name;
  }

}
