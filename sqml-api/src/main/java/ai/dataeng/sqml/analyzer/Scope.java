package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.analyzer.TypeResolver.RelationResolverContext;
import ai.dataeng.sqml.logical.LogicalPlan;
import ai.dataeng.sqml.logical.LogicalPlan.Builder;
import ai.dataeng.sqml.logical.QueryRelationDefinition;
import ai.dataeng.sqml.logical.RelationDefinition;
import ai.dataeng.sqml.logical.SubscriptionDefinition;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class Scope {
  private LogicalPlan.Builder logicalPlanBuilder;
  private RelationType currentRelation;
  private Optional<RelationDefinition> currentSqmlRelation;
  private Optional<Long> limit;

  public Scope() {
    this(null, new LogicalPlan.Builder(), Optional.empty(), Optional.empty());
  }

  public Scope(RelationType currentRelation, LogicalPlan.Builder logicalPlanBuilder,
      Optional<RelationDefinition> currentSqmlRelation, Optional<Long> limit) {
    this.currentRelation = currentRelation;
    this.logicalPlanBuilder = logicalPlanBuilder;
    this.currentSqmlRelation = currentSqmlRelation;
    this.limit = limit;
  }

  public static Scope.Builder builder() {
    return new Builder();
  }

  public Optional<Type> resolveType(QualifiedName name) {
    Set<Type> relations = getRelation().accept(
        new TypeResolver(), new RelationResolverContext(Optional.of(name), this));

    if (relations.size() > 1) {
      throw new RuntimeException(String.format("Ambiguous field %s", name));
    }
    if (relations.size() == 0) {
      return Optional.empty();
    }
    return relations.stream().findFirst();
  }

  public Optional<RelationDefinition> resolveRelation(QualifiedName name) {
    if (name.getParts().get(0).equalsIgnoreCase("@")) {
      QualifiedName entityName = currentSqmlRelation.orElseThrow(()->new RuntimeException("No base relation")).getRelationName();
      List<String> parts = new ArrayList<>(entityName.getParts());
      parts.addAll(name.getParts().subList(1, name.getParts().size()));
      name = QualifiedName.of(parts);
    }
    return logicalPlanBuilder.getCurrentDefinition(name);
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
    return (List<Field>)getRelation().getFields();
  }

  public RelationId getRelationId() {
    return null;
  }

  public RelationType<?> getRelation() {
    return currentRelation;
  }

  public Optional<RelationDefinition> getCurrentDefinition(QualifiedName entityName) {
    return logicalPlanBuilder.getCurrentDefinition(entityName);
  }

  public void setCurrentDefinition(QualifiedName name,
      RelationDefinition relationDefinition) {
    logicalPlanBuilder.setCurrentDefinition(name, relationDefinition);
  }

  public void setSubscriptionDefinition(SubscriptionDefinition subscriptionDefinition) {
    logicalPlanBuilder.setSubscriptionDefinition(subscriptionDefinition);

  }

  public void setMultiplicity(Optional<Long> limit) {
    this.limit = limit;
  }

  public Optional<Long> getLimit() {
    return limit;
  }

  public static class Builder {
    private Scope parent;
    private RelationType relationType;
    private Optional<RelationDefinition> currentSqmlRelation;
    private Optional<Long> limit;

    public Builder withParent(Scope scope) {
      this.parent = scope;
      return this;
    }

    public Builder withRelationType(RelationType relationType) {
      this.relationType = relationType;
      return this;
    }

    public Builder withCurrentSqmlRelation(Optional<RelationDefinition> currentSqmlRelation) {
      this.currentSqmlRelation = currentSqmlRelation;
      return this;
    }

    public Builder withMultiplicity(Optional<Long> limit) {
      this.limit = limit;
      return this;
    }

    public Scope build() {
      return new Scope(relationType, parent.logicalPlanBuilder, currentSqmlRelation, limit);
    }
  }
}
