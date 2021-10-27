package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import java.util.List;
import java.util.Optional;
import lombok.Getter;

public class LogicalPlan {

  private final RelationType<TypedField> root;

  public LogicalPlan(RelationType<TypedField> root) {
    this.root = root;
  }

  public RelationType<TypedField> getRoot() {
    return root;
  }

  @Getter
  public static class Builder {
    public RelationType<TypedField> root = new RelationType<>();
    public TypedField rootField = new StandardField(Name.system("root"), root, List.of(), Optional.empty());

    public static Type unbox(Type type) {
      if (type instanceof RelationType) {
        return type;
      }
      if (type instanceof ArrayType) {
        ArrayType arr = (ArrayType) type;
        return unbox(arr.getSubType());
      }
      return null;
    }

    public void addField(QualifiedName name, TypedField add) {
      TypedField field = getField(name)
          .orElseThrow(/*todo*/);

      RelationType type = (RelationType) unbox(field.getType());
      type.add(add);
    }

    public LogicalPlan build() {
      return new LogicalPlan(root);
    }

    public Optional<TypedField> getField(QualifiedName name) {
      TypedField field = this.rootField;

      List<String> parts = name.getParts();
      for (int i = 0; i < parts.size() - 1; i++) {
        String part = parts.get(i);
        Type type = unbox(field.getType());
        if (!(type instanceof RelationType)) {
          throw new RuntimeException("Could not find relation");
        }
        RelationType rel = (RelationType) type;
        Optional<TypedField> fieldOptional = rel.getField(part);
        if (fieldOptional.isEmpty()) {
          throw new RuntimeException("Could not find relation");
        }
        field = fieldOptional.get();
      }

      return Optional.ofNullable(field);
    }
  }
}
