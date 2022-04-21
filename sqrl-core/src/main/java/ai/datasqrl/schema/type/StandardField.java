package ai.datasqrl.schema.type;

import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.schema.type.constraint.Constraint;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@AllArgsConstructor
@Getter
public class StandardField implements TypedField {

  private final Name name;
  private final Type type;
  private final List<Constraint> constraints;

  @Override
  public String toString() {
    return name.getDisplay() + ":" + type.toString();
  }
}
