package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.analyzer.TypeResolver.RelationResolverContext;
import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import ai.dataeng.sqml.logical3.LogicalPlan2;
import ai.dataeng.sqml.logical3.LogicalPlan2.LogicalField;
import ai.dataeng.sqml.logical3.LogicalPlan2.SelectRelationField;
import ai.dataeng.sqml.schema2.Field;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.StandardField;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.basic.StringType;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class Scope {
  private RelationType currentRelation;
  private final LogicalPlan2.Builder planBuilder;
  private final QualifiedName contextName;
  private final Set<LogicalField> references = new HashSet<>();

  public Scope() {
    this(null, new LogicalPlan2.Builder(), QualifiedName.of());
  }

  public Scope(RelationType currentRelation, LogicalPlan2.Builder planBuilder,
      QualifiedName contextName) {
    this.currentRelation = currentRelation;
    this.planBuilder = planBuilder;
    this.contextName = contextName;
  }

  public static Scope.Builder builder() {
    return new Builder();
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

  public Optional<RelationType<LogicalField>> resolveRelation(QualifiedName name) {
    return planBuilder.resolveRelation(this.getContextName(), name);
  }

  public QualifiedName getContextName() {
    return contextName;
  }


  public List<LogicalField> resolveFieldsWithPrefix(Optional<QualifiedName> prefix) {
    if (prefix.isPresent()) {
      throw new RuntimeException("SELECT * with alias not yet implemented");
    }
    return (List<LogicalField>)getRelation().getFields();
  }

  public RelationId getRelationId() {
    return null;
  }

  public RelationType<LogicalField> getRelation() {
    return currentRelation;
  }

  public RelationType getRootRelation() {
    return this.planBuilder.getRoot();
  }

  public void addRootField(LogicalField field) {
    planBuilder.addRootField(field);
  }

  public void addField(LogicalField field) {
    RelationType<LogicalField> relationType =
        getContextName().getPrefix().map(n-> resolveRelation(n).orElseThrow()).orElseGet(
            this::getRootRelation);
    addField(field, relationType);
  }

  public void addField(LogicalField field, RelationType relationType) {
    planBuilder.addField(relationType, field);
  }

  public void addReference(LogicalField field) {
    this.references.add(field);
  }

  public static class Builder {
    private Scope parent;
    private RelationType relationType;
    private QualifiedName contextName;

    public Builder withParent(Scope scope) {
      this.parent = scope;
      return this;
    }

    public Builder withRelationType(RelationType relationType) {
      this.relationType = relationType;
      return this;
    }

    public Builder withContextName(QualifiedName contextName) {
      this.contextName = contextName;
      return this;
    }

    public Scope build() {
      return new Scope(relationType, parent.planBuilder, contextName);
    }

  }
}
