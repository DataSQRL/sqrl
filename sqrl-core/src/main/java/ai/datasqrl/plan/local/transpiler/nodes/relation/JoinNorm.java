package ai.datasqrl.plan.local.transpiler.nodes.relation;

import static java.util.Objects.requireNonNull;

import ai.datasqrl.parse.tree.AstVisitor;
import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Join;
import ai.datasqrl.parse.tree.Join.Type;
import ai.datasqrl.parse.tree.JoinOn;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.name.Name;
import ai.datasqrl.parse.tree.name.NamePath;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.util.List;
import java.util.Optional;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.apache.commons.collections.ListUtils;

/**
 * An normalized version of {@link ai.datasqrl.parse.tree.Join}
 */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
public class JoinNorm extends RelationNorm {

  private final Type type;
  @Setter
  private RelationNorm left;
  private final RelationNorm right;
  private final Optional<JoinOn> criteria;

  public JoinNorm(Optional<NodeLocation> location, Type type, RelationNorm left, RelationNorm right,
      Optional<JoinOn> criteria) {
    super(location);
    requireNonNull(left, "left is null");
    requireNonNull(right, "right is null");

    this.type = type;
    this.left = left;
    this.right = right;
    this.criteria = criteria;
  }

  public RelationNorm getLeftmost() {
    if (left instanceof JoinNorm) {
      return left.getLeftmost();
    }else if (left instanceof TableNodeNorm) {
      return left;
    } else {
      throw new RuntimeException(left.toString());
    }
  }

  public RelationNorm getRightmost() {
    if (right instanceof JoinNorm) {
      return right.getRightmost();
    }
    return right;
  }

  @Override
  public Name getFieldName(Expression references) {
    throw new RuntimeException("Not supported");
  }

  @Override
  public List<Expression> getFields() {
    throw new RuntimeException("Cannot get fields from a join norm");
  }

  @Override
  public List<Expression> getPrimaryKeys() {
    if (this.type == Type.LEFT) {
      return List.copyOf(left.getPrimaryKeys());
    }

    return Lists.newArrayList(Iterables.concat(left.getPrimaryKeys(), right.getPrimaryKeys()));
  }

  @Override
  public Optional<Expression> walk(NamePath namePath) {
    return Optional.empty();
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitJoinNorm(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return List.of(left, right);
  }
}
