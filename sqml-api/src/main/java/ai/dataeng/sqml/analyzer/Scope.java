package ai.dataeng.sqml.analyzer;

import ai.dataeng.sqml.analyzer.TypeResolver.RelationResolverContext;
import ai.dataeng.sqml.execution.importer.ImportSchema.Mapping;
import ai.dataeng.sqml.logical3.LogicalPlan2;
import ai.dataeng.sqml.logical3.LogicalPlan2.LogicalField;
import ai.dataeng.sqml.logical3.LogicalPlan2.ModifiableRelationType;
import ai.dataeng.sqml.schema2.RelationType;
import ai.dataeng.sqml.schema2.Type;
import ai.dataeng.sqml.schema2.name.Name;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Getter
public class Scope {
  private ModifiableRelationType currentRelation;
  private final LogicalPlan2.Builder planBuilder;
  private final QualifiedName contextName;

  public Scope() {
    this(null, new LogicalPlan2.Builder(), QualifiedName.of());
  }

  public Scope(ModifiableRelationType currentRelation, LogicalPlan2.Builder planBuilder,
      QualifiedName contextName) {
    this.currentRelation = currentRelation;
    this.planBuilder = planBuilder;
    this.contextName = contextName;
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

  public Optional<ModifiableRelationType<LogicalField>> resolveRelation(QualifiedName name) {
    return planBuilder.resolveModifiableRelation(this.getContextName(), name);
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

  public ModifiableRelationType<LogicalField> getRelation() {
    return currentRelation;
  }

  public void setImportRelation(Name name, Mapping mapping,
      RelationType relationType) {
    planBuilder.setImportRelation(name, mapping, relationType);
  }

  public ModifiableRelationType getRootRelation() {
    return this.planBuilder.getRoot();
  }

  public static class Builder {
    private Scope parent;
    private ModifiableRelationType relationType;
    private QualifiedName contextName;

    public Builder withParent(Scope scope) {
      this.parent = scope;
      return this;
    }

    public Builder withRelationType(ModifiableRelationType relationType) {
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
