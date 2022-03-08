package ai.dataeng.sqml.planner.operator2;

import static java.util.Objects.requireNonNull;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SqrlCalciteTable;

/**
 * Copied from calcite logical join
 */
public class SqrlJoin extends Join implements SqrlRelNode {
  private final boolean semiJoinDone;

  private final ImmutableList<RelDataTypeField> systemFieldList;

  public SqrlJoin(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode left,
      RelNode right,
      RexNode condition,
      Set<CorrelationId> variablesSet,
      JoinRelType joinType,
      boolean semiJoinDone,
      ImmutableList<RelDataTypeField> systemFieldList) {
    super(cluster, traitSet, hints, left, right, condition, variablesSet, joinType);
    this.semiJoinDone = semiJoinDone;
    this.systemFieldList = requireNonNull(systemFieldList, "systemFieldList");
  }

  public SqrlJoin(RelInput input) {
    this(input.getCluster(), input.getCluster().traitSetOf(Convention.NONE),
        new ArrayList<>(),
        input.getInputs().get(0), input.getInputs().get(1),
        requireNonNull(input.getExpression("condition"), "condition"),
        ImmutableSet.of(),
        requireNonNull(input.getEnum("joinType", JoinRelType.class), "joinType"),
        false,
        ImmutableList.of());
  }


  /** Creates a SqrlJoin. */
  public static SqrlJoin create(RelNode left, RelNode right, List<RelHint> hints,
      RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType) {
    return create(left, right, hints, condition, variablesSet, joinType, false,
        ImmutableList.of());
  }

  /** Creates a SqrlJoin, flagged with whether it has been translated to a
   * semi-join. */
  public static SqrlJoin create(RelNode left, RelNode right, List<RelHint> hints,
      RexNode condition, Set<CorrelationId> variablesSet, JoinRelType joinType,
      boolean semiJoinDone, ImmutableList<RelDataTypeField> systemFieldList) {
    final RelOptCluster cluster = left.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new SqrlJoin(cluster, traitSet, hints, left, right, condition,
        variablesSet, joinType, semiJoinDone, systemFieldList);
  }

  //~ Methods ----------------------------------------------------------------

  @Override public SqrlJoin copy(RelTraitSet traitSet, RexNode conditionExpr,
      RelNode left, RelNode right, JoinRelType joinType, boolean semiJoinDone) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new SqrlJoin(getCluster(),
        getCluster().traitSetOf(Convention.NONE), hints, left, right, conditionExpr,
        variablesSet, joinType, semiJoinDone, systemFieldList);
  }

  @Override
  public List<Field> getFields() {
    List<Field> newFields = new ArrayList<>();
    ((SqrlRelNode)stripHep(left)).getFields().forEach(newFields::add);
    ((SqrlRelNode)stripHep(right)).getFields().forEach(newFields::add);

    return newFields;
  }

  //TODO: Determine how to handle this better?
  public static RelNode stripHep(RelNode rel) {
    if (rel instanceof HepRelVertex) {
      HepRelVertex hepRelVertex = (HepRelVertex) rel;
      rel = hepRelVertex.getCurrentRel();
    }
    return rel;
  }


  /**
   * The primary keys are the fields on the side of the context table, we assume left
   */
  @Override
  public List<Column> getPrimaryKeys() {
    return ((SqrlRelNode)left).getPrimaryKeys();
  }

  @Override
  public List<Column> getReferences() {
    return null;
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    RelWriter w = super.explainTerms(pw);
    if (right instanceof SqrlTableScan && ((SqrlTableScan)right).getTable().getRowType() instanceof SqrlCalciteTable &&
        ((SqrlCalciteTable) ((SqrlTableScan)right).getTable().getRowType()).getRelationship() != null
    ) {
      w.item("rel", ((SqrlCalciteTable) ((SqrlTableScan)right).getTable().getRowType()).getRelationship().getId());
    }

    return w
        .item("fields", this.getFields().stream().map(e->e.getName()).collect(Collectors.toList()))
        ;
  }
}
