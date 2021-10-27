package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.Optional;
import lombok.Getter;

@Getter
public class ExpressionField implements TypedField {

  private final Name name;
  private final Type type;
  private final Optional<String> alias;

  public <E, T> ExpressionField(Name name, Type type, Optional<String> alias) {
    this.name = name;
    this.type = type;
    this.alias = alias;
  }

  @Override
  public Field withAlias(String alias) {
    return new ExpressionField(name, type, Optional.of(alias));
  }
}
