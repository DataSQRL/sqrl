package ai.datasqrl.plan.local.transpiler.nodes.node;

import static java.util.Objects.requireNonNull;

import ai.datasqrl.parse.tree.Expression;
import ai.datasqrl.parse.tree.Node;
import ai.datasqrl.parse.tree.NodeLocation;
import ai.datasqrl.parse.tree.Select;
import ai.datasqrl.parse.tree.SingleColumn;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import lombok.EqualsAndHashCode;
import lombok.Getter;

/**
 * An normalized version of {@link ai.datasqrl.parse.tree.Select}
 */
@EqualsAndHashCode(callSuper = false)
@Getter
public class SelectNorm
    extends Node {
  private final boolean distinct;
  private final List<SingleColumn> selectItems;

  public SelectNorm(Optional<NodeLocation> location, boolean distinct,
      List<SingleColumn> selectItems) {
    super(location);
    this.distinct = distinct;
    this.selectItems = ImmutableList.copyOf(requireNonNull(selectItems, "selectItems"));
  }

  public static SelectNorm create(Select node, List<SingleColumn> columns) {
    return new SelectNorm(node.getLocation(), node.isDistinct(), columns);
  }

  @Override
  public List<? extends Node> getChildren() {
    return selectItems;
  }

  public List<Expression> getAsExpressions() {
    return selectItems.stream().map(SingleColumn::getExpression).collect(Collectors.toList());
  }
}
