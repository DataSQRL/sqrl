package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.SimpleName;
import java.util.Optional;
import lombok.Getter;

@Getter
public class QueryField implements Field {

  private final Optional<String> name;
  private final RelationDefinition type;

  public QueryField(String name, RelationDefinition type) {
    this.name = Optional.of(name);
    this.type = type;
  }

  public Name getName() {
    return new SimpleName(name.get());
  }
}
