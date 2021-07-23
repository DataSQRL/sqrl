package ai.dataeng.sqml.parser;

import ai.dataeng.sqml.tree.AstVisitor;
import ai.dataeng.sqml.tree.Node;
import ai.dataeng.sqml.tree.NodeLocation;
import ai.dataeng.sqml.tree.QualifiedName;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

public class CreateRelationship extends Node {

  private final QualifiedName name;
  private final QualifiedName relation;
  private final Node expression;
  private final Optional<QualifiedName> inverse;
  private final Optional<String> limit;

  protected CreateRelationship(Optional<NodeLocation> location,
      QualifiedName name, QualifiedName relation,
      Node expression, Optional<QualifiedName> inverse,
      Optional<String> limit) {
    super(location);
    this.name = name;
    this.relation = relation;
    this.expression = expression;
    this.inverse = inverse;
    this.limit = limit;
  }

  public QualifiedName getName() {
    return name;
  }

  public QualifiedName getRelation() {
    return relation;
  }

  public Node getExpression() {
    return expression;
  }

  public Optional<QualifiedName> getInverse() {
    return inverse;
  }

  public Optional<String> getLimit() {
    return limit;
  }
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitCreateRelationship(this, context);
  }
  @Override
  public List<? extends Node> getChildren() {
    return null;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    CreateRelationship that = (CreateRelationship) o;
    return Objects.equals(name, that.name) && Objects
        .equals(relation, that.relation) && Objects.equals(expression, that.expression)
        && Objects.equals(inverse, that.inverse) && Objects
        .equals(limit, that.limit);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, relation, expression, inverse, limit);
  }

  @Override
  public String toString() {
    return "CreateRelationship{" +
        "name=" + name +
        ", relation=" + relation +
        ", expression=" + expression +
        ", inverse=" + inverse +
        ", limit=" + limit +
        '}';
  }
}
