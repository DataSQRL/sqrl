package ai.dataeng.sqml.planner.operator2;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import com.google.common.collect.ImmutableSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMdCollation;
import org.apache.calcite.rel.metadata.RelMdDistribution;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class SqrlFilter extends Filter implements SqrlRelNode {
  private final Set<CorrelationId> variablesSet;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a SqrlFilter.
   *
   * <p>Use {@link #create} unless you know what you're doing.
   *
   * @param cluster   Cluster that this relational expression belongs to
   * @param child     Input relational expression
   * @param condition Boolean expression which determines whether a row is
   *                  allowed to pass
   * @param variablesSet Correlation variables set by this relational expression
   *                     to be used by nested expressions
   */
  public SqrlFilter(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelNode child,
      RexNode condition,
      Set<CorrelationId> variablesSet) {
    super(cluster, traitSet, child, condition);
    this.variablesSet = Objects.requireNonNull(variablesSet, "variablesSet");
  }

  /**
   * Creates a SqrlFilter by parsing serialized output.
   */
  public SqrlFilter(RelInput input) {
    super(input);
    this.variablesSet = ImmutableSet.of();
  }

  /** Creates a SqrlFilter. */
  public static SqrlFilter create(final RelNode input, RexNode condition) {
    return create(input, condition, ImmutableSet.of());
  }

  /** Creates a SqrlFilter. */
  public static SqrlFilter create(final RelNode input, RexNode condition,
      Set<CorrelationId> variablesSet) {
    final RelOptCluster cluster = input.getCluster();
    final RelMetadataQuery mq = cluster.getMetadataQuery();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE)
        .replaceIfs(RelCollationTraitDef.INSTANCE,
            () -> RelMdCollation.filter(mq, input))
        .replaceIf(RelDistributionTraitDef.INSTANCE,
            () -> RelMdDistribution.filter(mq, input));
    return new SqrlFilter(cluster, traitSet, input, condition, variablesSet);
  }

  @Override public SqrlFilter copy(RelTraitSet traitSet, RelNode input,
      RexNode condition) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new SqrlFilter(getCluster(), traitSet, input, condition,
        variablesSet);
  }

  @Override
  public List<Field> getFields() {
    return ((SqrlRelNode) input).getFields();
  }

  @Override
  public List<Column> getPrimaryKeys() {
    return ((SqrlRelNode) input).getPrimaryKeys();
  }

  @Override
  public List<Column> getReferences() {
    return ((SqrlRelNode) input).getReferences();
  }
}
