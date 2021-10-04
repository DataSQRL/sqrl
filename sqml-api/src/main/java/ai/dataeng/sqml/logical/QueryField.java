package ai.dataeng.sqml.logical;

import ai.dataeng.sqml.analyzer.Field;
import java.util.Optional;
import lombok.Getter;

@Getter
public class QueryField extends Field {

  private final Optional<String> name;

  public QueryField(String name, RelationDefinition type) {
    super(type);
    this.name = Optional.of(name);
  }
}
