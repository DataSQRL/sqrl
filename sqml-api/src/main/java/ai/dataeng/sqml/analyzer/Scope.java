package ai.dataeng.sqml.analyzer;

import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;

import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class Scope {
  private Optional<Scope> parent;
  private final Optional<TypedField> field;
  private boolean queryBoundary = false;
  private RelationType<TypedField> relation;

  public Scope() {
    this(new RelationType(), Optional.empty());
  }

  public Scope(RelationType relation, Optional<Scope> parent) {
    this(relation, parent, Optional.empty());
  }

  public Scope(RelationType relation, Optional<Scope> parent, Optional<TypedField> field) {

    this.relation = relation;
    this.parent = parent;
    this.field = field;
  }

  public static Scope.Builder builder() {
    return new Builder();
  }

  public static Scope create(Optional<TypedField> field) {
    return new Scope(new RelationType(), Optional.empty(), field);
  }

  public static Scope create(RelationType relationType, Optional<TypedField> field) {
    return new Scope(relationType, Optional.empty(), field);
  }

  public List<TypedField> resolveFieldsWithPrefix(Optional<QualifiedName> prefix) {
    if (prefix.isPresent()) {
      throw new RuntimeException("SELECT * with alias not yet implemented");
    }
    //todo prefix
    return (List<TypedField>)getRelation().getFields();
  }

  public RelationId getRelationId() {
    return null;
  }

  public RelationType<TypedField> getRelation() {
    return relation;
  }

  //Returns all complete paths
  public List<FieldPath> toFieldPath(QualifiedName name) {


    return toFieldPath(name, relation);
  }
  public List<FieldPath> toFieldPath(QualifiedName name, RelationType<TypedField> relation) {
    List<TypedField> matches = new ArrayList<>();
    for (TypedField field : relation) {
      if (field.canResolvePrefix(name).isPresent()) {
        matches.add(field);
      }
    }

    List<FieldPath> paths = new ArrayList<>();
    for (TypedField field : matches) {
      QualifiedName prefix = field.canResolvePrefix(name).get();

      QualifiedName withoutPrefix = name.withoutPrefix(prefix);
      FieldPath fieldPath = FieldPath.of(field);
      if (withoutPrefix.getParts().size() == 0) { //perfect match
        paths.add(fieldPath);
      } else if (unbox(field.getType()) instanceof RelationType) { //is pathable
        List<FieldPath> nextPaths = toFieldPath(withoutPrefix, (RelationType<TypedField>) unbox(field.getType()));
        for (FieldPath path : nextPaths) {
          paths.add(path.prepend(field));
        }
      }
    }


    return paths;
  }

  public static class Builder {
    private Scope parent;
    private RelationType relationType;

    public Builder withParent(Scope scope) {
      this.parent = scope;
      return this;
    }

    public Builder withRelationType(RelationType relationType) {
      this.relationType = relationType;
      return this;
    }

    public Scope build() {
      return new Scope(relationType, Optional.ofNullable(parent));
    }

  }
}
