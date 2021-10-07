package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.schema2.name.SimpleName;
import java.util.Optional;
import lombok.Getter;

public class QueryField implements Field {

  private final Optional<String> name;

  public QueryField(String name, RelationDefinition type) {
//    super(type);
    this.name = Optional.of(name);
  }

  public Name getName() {
    return new SimpleName(name.get());
  }
}
