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

  public RelationSqmlType createRelation(Optional<QualifiedName> name) {
    if (name.isEmpty()) {
      return root;
    }

    QualifiedName relName = dereferenceName(name.get());

    RelationSqmlType rel;
    if (relName.getPrefix().isPresent()) {
      rel = createRelation(relName.getPrefix());
    } else {
      rel = root;
    }

    Optional<Field> field = rel.resolveField(QualifiedName.of(relName.getSuffix()),
        rel);
    if (field.isEmpty()) {
      RelationSqmlType newRel = new RelationSqmlType(relName);
      rel.addField(Field.newUnqualified(relName.getSuffix(), newRel));
      return newRel;
    }
    Preconditions.checkState(field.get().getType() instanceof RelationSqmlType,
        "Mismatched fields. Expecting relation %s, got %s", relName,
        field.get().getType().getClass().getName());
    return (RelationSqmlType) field.get().getType();
  }

  public Optional<RelationSqmlType> getRelation(QualifiedName name) {
    name = dereferenceName(name);

    RelationSqmlType rel = root;
    List<String> parts = name.getParts();
    for (String part : parts) {
      Optional<Field> field = rel.resolveField(QualifiedName.of(part), rel);
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

  public QualifiedName dereferenceName(QualifiedName name) {
    if (name.getParts().get(0).equalsIgnoreCase("@")) {
      List<String> newName = new ArrayList<>(getName().getParts().subList(0, getName().getParts().size() - 1));
      newName.addAll(name.getParts().subList(1, name.getParts().size()));
      return dereferenceParentName(QualifiedName.of(newName));
    }

    return dereferenceParentName(name);
  }

  private QualifiedName dereferenceParentName(QualifiedName name) {
    List<String> parts = new ArrayList<>(name.getParts());
    for (int i = parts.size() - 1; i >= 0; i--) {
      if (parts.get(i).equalsIgnoreCase("parent")) {
        parts.remove(i);
        parts.remove(i);
        i--;
      } else if (parts.get(i).equalsIgnoreCase("siblings")) {
        parts.remove(i);
      }
    }

    return QualifiedName.of(parts);
  }

  public List<Field> resolveFields(QualifiedName name) {
    return getRelationType().resolveFields(dereferenceName(name), relationType);
  }

  public Optional<Field> resolveField(QualifiedName name) {
    if (name.getParts().get(0).equalsIgnoreCase("@")) {
      return getRelationType().resolveField(dereferenceName(name), root);
    }
    return getRelationType().resolveField(dereferenceName(name), this.getRelationType());
  }

  public RelationSqmlType getRoot() {
    return root;
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