package ai.dataeng.sqml.planner.operator2;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexNode;
import org.checkerframework.checker.nullness.qual.Nullable;

public class SqrlSort extends Sort implements SqrlRelNode {
  private SqrlSort(RelOptCluster cluster, RelTraitSet traitSet,
      RelNode input, RelCollation collation, @Nullable RexNode offset, @Nullable RexNode fetch) {
    super(cluster, traitSet, input, collation, offset, fetch);
    assert traitSet.containsIfApplicable(Convention.NONE);
  }

  /**
   * Creates a LogicalSort by parsing serialized output.
   */
  public SqrlSort(RelInput input) {
    super(input);
  }

  public static SqrlSort create(RelNode input, RelCollation collation,
      @Nullable RexNode offset, @Nullable RexNode fetch) {
    RelOptCluster cluster = input.getCluster();
    collation = RelCollationTraitDef.INSTANCE.canonize(collation);
    RelTraitSet traitSet =
        input.getTraitSet().replace(Convention.NONE).replace(collation);
    return new SqrlSort(cluster, traitSet, input, collation, offset, fetch);
  }

  @Override public Sort copy(RelTraitSet traitSet, RelNode newInput,
      RelCollation newCollation, @Nullable RexNode offset, @Nullable RexNode fetch) {
    return new SqrlSort(getCluster(), traitSet, newInput, newCollation,
        offset, fetch);
  }

  @Override
  public List<Field> getFields() {
    return ((SqrlRelNode)input).getFields();
  }

  @Override
  public List<Column> getPrimaryKeys() {
    return ((SqrlRelNode)input).getPrimaryKeys();
  }

  @Override
  public List<Column> getReferences() {
    return null;
  }
}
