package ai.dataeng.sqml.analyzer;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.Iterables.find;
import static com.google.common.collect.Iterables.getOnlyElement;

import ai.dataeng.sqml.analyzer.TypeResolver.RelationResolverContext;
import ai.dataeng.sqml.tree.Expression;
import ai.dataeng.sqml.tree.QualifiedName;
import ai.dataeng.sqml.type.RelationType.RootRelationType;
import ai.dataeng.sqml.type.Type;
import ai.dataeng.sqml.type.RelationType;
import com.google.common.base.Preconditions;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.logging.Logger;

public class Scope {
  private QualifiedName name;
  private Optional<Scope> parent;
  private RelationType relation;
  private final RelationType root;
  private Logger log = Logger.getLogger(Scope.class.getName());

  public Scope(RootRelationType root) {
    this.root = root;
    this.relation = root;
  }

  public Scope(QualifiedName name) {
    this.name = name;
    this.root = new RelationType();
    this.relation = root;
  }

  public Scope(QualifiedName name, Optional<Scope> parent, RelationType relation,
      RelationType root) {
    checkState(relation != null, "Relation cannot be null");
    this.name = name;
    this.parent = parent;
    this.relation = relation;
    this.root = root;
  }

  public static Scope.Builder builder() {
    return new Builder();
  }

  /**
   * Creates a relation on the given name
   */
  public RelationType getRoot() {
    return root;
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

  public Optional<RelationType> resolveRelation(QualifiedName name) {
    Set<Type> relations = getRelation().accept(
        new TypeResolver(), new RelationResolverContext(Optional.of(name), this));

    if (relations.size() > 1) {
      throw new RuntimeException(String.format("Ambigious field %s", name));
    }
    if (relations.size() == 0) {
      return Optional.empty();
    }
    Preconditions.checkState(relations.stream().findFirst().get() instanceof RelationType,
        "Relation not a relational type: %s found %s", name, relations.stream().findFirst().get());
    return Optional.of((RelationType)relations.stream().findFirst().get());
  }

  public void addRelation(QualifiedName name, RelationType relation) {
    RelationType rel = name.getPrefix().map(p-> resolveRelation(p)
            .orElseThrow(()->new RuntimeException(String.format("Could not find relation prefix %s", name))))
        .orElse(root);
    relation.setParent(rel); //todo This isn't a great place to do this...
    rel.addField(Field.newUnqualified(name.getSuffix(), relation));
  }

  public void addFunction(QualifiedName name) {

  }

  public void addRootField(Field field) {
    this.root.addField(field);
  }

  public List<Field> resolveFieldsWithPrefix(Optional<QualifiedName> prefix) {
    if (prefix.isPresent()) {
      throw new RuntimeException("SELECT * with alias not yet implemented");
    }
    return getRelation().getFields();
  }

  public RelationType getRelationType() {
    return null;
  }

  public RelationId getRelationId() {
    return null;
  }

  public static class Builder {
    private Scope parent;
    private RelationType relationType;
    private QualifiedName name;

    public Builder withName(QualifiedName name) {
      this.name = name;
      return this;
    }
    public Builder withParent(Scope scope) {
      this.parent = scope;
      return this;
    }

    public Builder withRelationType(RelationType relationType) {
      this.relationType = relationType;
      return this;
    }

    public Scope build() {
      return new Scope(name, Optional.ofNullable(parent), relationType, parent.root);
    }
  }

  public QualifiedName getName() {
    return name;
  }

  public RelationType getRelation() {
    return relation;
  }
}
