package ai.dataeng.sqml.parser.validator;

import ai.dataeng.sqml.type.TypedField;
import java.util.ArrayList;
import java.util.List;
import lombok.Value;

@Value
public class FieldPath {
  List<TypedField> fields;

  public static FieldPath of(TypedField field) {
    return new FieldPath(List.of(field));
  }

  public FieldPath prepend(TypedField field) {
    List<TypedField> f = new ArrayList<>(fields.size() + 1);
    f.add(field);
    f.addAll(fields);
    return new FieldPath(f);
  }
}
