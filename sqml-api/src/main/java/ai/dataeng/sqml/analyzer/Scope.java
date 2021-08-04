package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.SqmlType.RelationSqmlType;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class Scope {
  private QualifiedName name;
  private Scope parent;
  private Node node;
  private RelationSqmlType relationType;
  private final RelationSqmlType root;

  public Scope() {
    this.root = new RelationSqmlType();
  }
  public Scope(QualifiedName name) {
    this.name = name;
    this.root = new RelationSqmlType();
  }

  public Scope(QualifiedName name, Scope parent, Node node, RelationSqmlType relationType,
      RelationSqmlType root) {
    this.name = name;
    this.parent = parent;
    this.node = node;
    this.relationType = relationType;
    this.root = root;
  }

  public static Scope.Builder builder() {
    return new Builder();
  }

  public RelationSqmlType createRelation(QualifiedName name) {
    name = dereferenceTableName(name);

    RelationSqmlType rel;
    if (name.getPrefix().isPresent()) {
      rel = createRelation(name.getPrefix().get());
    } else {
      rel = root;
    }

    Optional<Field> field = rel.resolveField(QualifiedName.of(name.getSuffix()));
    if (field.isEmpty()) {
      RelationSqmlType newRel = new RelationSqmlType(name);
      rel.addField(Field.newUnqualified(name.getSuffix(), newRel));
      return newRel;
    }
    Preconditions.checkState(field.get().getType() instanceof RelationSqmlType,
        "Mismatched fields. Expecting relation %s, got %s", name,
        field.get().getType().getClass().getName());
    return (RelationSqmlType) field.get().getType();
  }

  public Optional<RelationSqmlType> getRelation(QualifiedName name) {
    name = dereferenceTableName(name);

    RelationSqmlType rel = root;
    List<String> parts = name.getParts();
    for (String part : parts) {
      Optional<Field> field = rel.resolveField(QualifiedName.of(part));
      if (field.isEmpty()) {
        throw new RuntimeException(String.format("Name cannot be found %s", name));
      }
      if (!(field.get().getType() instanceof RelationSqmlType)) {
        throw new RuntimeException(String.format("Name not a relation %s", name));
      }
      rel = (RelationSqmlType) field.get().getType();
    }
    return Optional.of(rel);
  }

  private QualifiedName dereferenceTableName(QualifiedName name) {
    if (name.getParts().get(0).equalsIgnoreCase("@")) {
      List<String> newName = new ArrayList<>(getName().getParts().subList(0, getName().getParts().size() - 1));
      newName.addAll(name.getParts().subList(1, name.getParts().size()));
      return QualifiedName.of(newName);
    }

    return name;
  }

  public static class Builder {

    private Scope parent;
    private Node node;
    private RelationSqmlType relationType;
    private QualifiedName name;

    public Builder withName(QualifiedName name) {
      this.name = name;
      return this;
    }
    public Builder withParent(Scope scope) {
      this.parent = scope;
      return this;
    }

    public Builder withRelationType(Node node,
        RelationSqmlType relationType) {
      this.node = node;
      this.relationType = relationType;
      return this;
    }

    public Scope build() {
      return new Scope(name, parent, node, relationType, parent.root);
    }
  }

  public QualifiedName getName() {
    return name;
  }

  public RelationSqmlType getRelationType() {
    return relationType;
  }
}