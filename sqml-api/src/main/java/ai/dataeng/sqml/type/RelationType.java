package ai.dataeng.sqml.type;

import ai.dataeng.sqml.analyzer.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

public class RelationType extends Type {
  public static RelationType INSTANCE = new RelationType();

  protected List<Field> fields;

  public RelationType() {
    this(new ArrayList<>());
  }

  public RelationType(List<Field> fields) {
    super("RELATION");
    this.fields = fields;
  }

  public <R, C> R accept(SqmlTypeVisitor<R, C> visitor, C context) {
    return visitor.visitRelation(this, context);
  }

  public List<Field> getFields() {
    return fields;
  }
//
//  public List<Field> getVisibleFields() {
//    return fields.stream()
//        .filter(f->!f.isHidden())
//        .collect(Collectors.toList());
//  }

  public Optional<Field> getField(String name) {
    for (Field field : getFields()) {
      if (field.getName().isPresent() && field.getName().get().equalsIgnoreCase(name)) {
        return Optional.of(field);
      }
    }
    return Optional.empty();
  }

  public int indexOf(Field field) {
    for (int i = 0; i < getFields().size(); i++) {
      if(fields.get(i) == field) {
        return i;
      }
    }
    return -1;
  }
}
