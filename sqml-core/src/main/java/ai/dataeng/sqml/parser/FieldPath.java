package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.NamePath;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Value;

@Value
public class FieldPath {
  private List<Field> fields;

  public static FieldPath of(Field... fields) {
    return new FieldPath(List.of(fields));
  }

  public String getName() {
    return fields.stream().map(e->e.getName().getDisplay()).collect(Collectors.joining("."));
  }

  public Field getLastField() {
    if (fields.isEmpty()) {
      return null;
    }
    return fields.get(fields.size() - 1);
  }

  public NamePath qualify() {
    return NamePath.of(this.fields.stream()
        .map(e->e.getId())
        .collect(Collectors.toList()));
  }
}
