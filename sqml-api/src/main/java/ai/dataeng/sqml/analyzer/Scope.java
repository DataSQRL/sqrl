package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.analyzer.TypeResolver.RelationResolverContext;
import ai.dataeng.sqml.logical.LogicalPlan;
import ai.dataeng.sqml.logical.RelationDefinition;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.RelationType;
import ai.dataeng.sqml.type.Type;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;


//TODO: ADD:
// 1. Functions should be registered here. Functions are scoped within scripts. ( can be aliased )
@Slf4j
@Getter
public class Scope {
  private LogicalPlan logicalPlan;
  private RelationType currentRelation;
  private RelationDefinition currentSqmlRelation;

  public Scope(LogicalPlan logicalPlan) {
    this(null, logicalPlan, null);
  }
  public Scope(RelationType currentRelation, LogicalPlan logicalPlan,
      RelationDefinition currentSqmlRelation) {
    this.currentRelation = currentRelation;
    this.logicalPlan = logicalPlan;
    this.currentSqmlRelation = currentSqmlRelation;
  }

  public static Scope.Builder builder() {
    return new Builder();
  }

  public Optional<Type> resolveType(QualifiedName name) {
    Set<Type> relations = getRelation().accept(
        new TypeResolver(), new RelationResolverContext(Optional.of(name), this));

    if (relations.size() > 1) {
      throw new RuntimeException(String.format("Ambigious field %s", name));
    }
    if (relations.size() == 0) {
      return Optional.empty();
    }
    return relations.stream().findFirst();
  }

  public Optional<RelationDefinition> resolveRelation(QualifiedName name) {
    if (name.getParts().get(0).equalsIgnoreCase("@")) {
      QualifiedName entityName = currentSqmlRelation.getRelationName();
      List<String> parts = new ArrayList<>(entityName.getParts());
      parts.addAll(name.getParts().subList(1, name.getParts().size()));
      name = QualifiedName.of(parts);
    }
    return logicalPlan.getCurrentDefinition(name);
//    Set<Type> relations = getRelation().accept(
//        new TypeResolver(), new RelationResolverContext(Optional.of(name), this));

//    if (relations.size() > 1) {
//      throw new RuntimeException(String.format("Ambigious field %s", name));
//    }
//    if (tableDefinition.isEmpty()) {
//      return Optional.empty();
//    }
//    Preconditions.checkState(relations.stream().findFirst().get() instanceof RelationType,
//        "Relation not a relational type: %s found %s", name, relations.stream().findFirst().get());
//    return Optional.of((RelationType)relations.stream().findFirst().get());
  }

//  public void addRelation(QualifiedName name, RelationType relation) {
//    RelationType rel = name.getPrefix().map(p-> resolveRelation(p)
//            .orElseThrow(()->new RuntimeException(String.format("Could not find relation prefix %s", name))))
//        .orElse(root);
//    relation.setParent(rel); //todo This isn't a great place to do this...
//    rel.addField(Field.newUnqualified(name.getSuffix(), relation));
//  }

//  public void addFunction(QualifiedName name) {
//
//  }

//  public void addRootField(Field field) {
//    this.root.addField(field);
//  }

  public List<Field> resolveFieldsWithPrefix(Optional<QualifiedName> prefix) {
    if (prefix.isPresent()) {
      throw new RuntimeException("SELECT * with alias not yet implemented");
    }
    return getRelation().getFields();
  }

  public RelationId getRelationId() {
    return null;
  }

  public void addRelation(String name, RelationType relation) {
  }

  public RelationType getRelation() {
    return currentRelation;
  }

  public static class Builder {
    private Scope parent;
    private RelationType relationType;
    private LogicalPlan logicalPlan;
    private RelationDefinition currentSqmlRelation;

    public Builder withParent(Scope scope) {
      this.parent = scope;
      return this;
    }

    public Builder withRelationType(RelationType relationType) {
      this.relationType = relationType;
      return this;
    }

    public Builder withCurrentSqmlRelation(RelationDefinition currentSqmlRelation) {
      this.currentSqmlRelation = currentSqmlRelation;
      return this;
    }

    public Builder withLogicalPlan(LogicalPlan logicalPlan) {
      this.logicalPlan = logicalPlan;
      return this;
    }

    public Scope build() {
      return new Scope(relationType, logicalPlan, currentSqmlRelation);
    }
  }
}
