package ai.dataeng.sqml.logical3;

import ai.dataeng.sqml.schema2.ArrayType;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.tree.name.Name;
import com.google.common.base.Preconditions;
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
          .orElse(rootField);

      RelationType type = (RelationType) unbox(field.getType());
      type.add(add);
    }

    public void addFieldPrefix(QualifiedName name, TypedField add) {
      TypedField field = name.getPrefix().map(f->getField(f).orElseThrow(/*todo*/))
          .orElse(rootField);

      RelationType type = (RelationType) unbox(field.getType());
      type.add(add);
    }

    public LogicalPlan build() {
      return new LogicalPlan(root);
    }

    public Optional<TypedField> resolveTableField(QualifiedName tableName, Optional<TypedField> fieldScope) {
      RelationType relation;
      if (tableName.getParts().get(0).equals("@")) {
        if (fieldScope.isEmpty()) {
          throw new RuntimeException("Cannot use @ expression here");
        }
        QualifiedName postfix = QualifiedName.of(tableName.getOriginalParts().subList(1, tableName.getOriginalParts().size()));
        TypedField field = getField(postfix, fieldScope.get())
            .orElseThrow(/*todo*/);
//        Preconditions.checkState(unbox(field.getType()) instanceof RelationType, "Must be rel type %s", field.getType().getClass().getName());
        return Optional.of(field);
      } else {
        TypedField field = getField(tableName)
            .orElseThrow(/*todo*/);
        return Optional.of(field);
      }
    }

    public Optional<TypedField> getField(QualifiedName name) {
      return getField(name, this.rootField);
    }
    public Optional<TypedField> getField(Optional<QualifiedName> name) {
      if (name.isEmpty()) {
        return Optional.of(this.rootField);
      }
      return getField(name.get(), this.rootField);
    }
    public Optional<TypedField> getField(QualifiedName name, TypedField field) {
      List<String> parts = name.getParts();
      for (int i = 0; i < parts.size(); i++) {
        String part = parts.get(i);
        Type type = unbox(field.getType());
        if (!(type instanceof RelationType)) {
          return Optional.empty();
        }
        RelationType rel = (RelationType) type;
        Optional<TypedField> fieldOptional = rel.getField(part);
        if (fieldOptional.isEmpty()) {
          return Optional.empty();
        }
        field = fieldOptional.get();
      }

      return Optional.ofNullable(field);
    }
  }

  @Override
  public String toString() {
    return "LogicalPlan{" +
        "root=" + root +
        '}';
  }
}
