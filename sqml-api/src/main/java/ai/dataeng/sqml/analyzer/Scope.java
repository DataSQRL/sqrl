package ai.dataeng.sqml.analyzer;

import static ai.dataeng.sqml.logical3.LogicalPlan.Builder.unbox;

import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.TypedField;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.tree.QualifiedName;
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
  private RelationType relation;

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

  public Optional<Type> resolveType(QualifiedName name) {
    return Optional.of(new StringType());
//
//    Set<Type> relations = getRelation().accept(
//        new TypeResolver(), new RelationResolverContext(Optional.of(name), this));
//
//    if (relations.size() > 1) {
//      throw new RuntimeException(String.format("Ambiguous field %s", name));
//    }
//    if (relations.size() == 0) {
//      return Optional.empty();
//    }
//    return relations.stream().findFirst();
  }

  public List<TypedField> resolveFieldsWithPrefix(Optional<QualifiedName> prefix) {
    if (prefix.isPresent()) {
      throw new RuntimeException("SELECT * with alias not yet implemented");
    }
    return (List<TypedField>)getRelation().getFields();
  }

  public RelationId getRelationId() {
    return null;
  }

  public RelationType<TypedField> getRelation() {
    return relation;
  }

  public Optional<TypedField> getField(QualifiedName name) {
//    Optional<TypedField> rootField = getField(name, planBuilder.rootField);
//    Optional<TypedField> localField = getField(name, new DataField(name.getSuffix(), this.getRelation(), List.of()));
//
//    Set<TypedField> fields = new HashSet<>();
//    rootField.map(fields::add);
//    localField.map(fields::add);
//
//    if (fields.size() > 1) {
//      throw new RuntimeException("Ambiguous column name:" + name);
//    } else if (rootField.isPresent()) {
//      return rootField;
//    } else {
//      return localField;
//    }
    return Optional.empty();
  }

  public Optional<TypedField> getField(QualifiedName name, TypedField field) {
    //todo: Fix aliasing
    for (String part : name.getParts()) {
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

    return Optional.of(field);
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
