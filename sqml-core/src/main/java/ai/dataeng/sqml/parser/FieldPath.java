package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.name.Name;
import ai.dataeng.sqml.tree.name.NamePath;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.Value;

/**
 * Defines a
 */
@Value
public class FieldPath {
  private List<Field> fields;

  public static FieldPath of(Field... fields) {
    return new FieldPath(List.of(fields));
  }

  public static Optional<FieldPath> walkPath(Table table, NamePath path) {
    List<Field> fieldPath = new ArrayList<>();
    Table toTable = table;
    for (Name fieldName : path) {
      Field field = toTable.getField(fieldName);
      Preconditions.checkState(field instanceof Relationship,
          "Field is not a relationship: %s is %s", fieldName, field);
      Relationship rel = (Relationship) field;
      toTable = rel.getToTable();
      fieldPath.add(rel);
    }

    if (fieldPath.isEmpty()) {
      return Optional.empty();
    }
    return Optional.of(new FieldPath(fieldPath));
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
}
