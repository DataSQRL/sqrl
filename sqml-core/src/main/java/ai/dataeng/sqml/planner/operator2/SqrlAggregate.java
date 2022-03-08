package ai.dataeng.sqml.planner.operator2;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.RowToColumnConverter;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.util.ImmutableBitSet;
import org.checkerframework.checker.nullness.qual.Nullable;
import scala.annotation.meta.field;

public class SqrlAggregate extends Aggregate implements SqrlRelNode {
  public SqrlAggregate(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    super(cluster, traitSet, hints, input, groupSet, groupSets, aggCalls);
  }
  /** Creates a LogicalAggregate. */
  public static SqrlAggregate create(final RelNode input,
      List<RelHint> hints,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    return create_(input, hints, groupSet, groupSets, aggCalls);
  }

  private static SqrlAggregate create_(final RelNode input,
      List<RelHint> hints,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets,
      List<AggregateCall> aggCalls) {
    final RelOptCluster cluster = input.getCluster();
    final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
    return new SqrlAggregate(cluster, traitSet, hints, input, groupSet,
        groupSets, aggCalls);
  }

  @Override public SqrlAggregate copy(RelTraitSet traitSet, RelNode input,
      ImmutableBitSet groupSet,
      @Nullable List<ImmutableBitSet> groupSets, List<AggregateCall> aggCalls) {
    assert traitSet.containsIfApplicable(Convention.NONE);
    return new SqrlAggregate(getCluster(), traitSet, hints, input,
        groupSet, groupSets, aggCalls);
  }

  /**
   * Fields are the group sets as primary keys & agg calls as new columns
   */
  @Override
  public List<Field> getFields() {
    List<Field> fields = new ArrayList<>();
//    getPrimaryKeys().forEach(fields::add);
    List<AggregateCall> calls = this.aggCalls;
    for (int group : groupSet) {
      RelDataTypeField field = this.getRowType().getFieldList().get(group);
//      fields.add(((SqrlRelNode)input).getFields().get(group));
      fields.add(Column.createTemp(field.getName().split("\\$")[0],
          RowToColumnConverter.toBasicType(field.getType()), null));
    }

    for (int i = 0; i < calls.size(); i++) {
      AggregateCall aggregateCall = calls.get(i);
      RelDataTypeField field = this.getRowType().getFieldList().get(this.getGroupCount()+i);
      fields.add(Column.createTemp(field.getName(),
          RowToColumnConverter.toBasicType(field.getType()), null));
    }

    return fields;
  }

  /**
   * Group sets
   */
  @Override
  public List<Column> getPrimaryKeys() {
    SqrlRelNode rel = (SqrlRelNode)this.input;
    List<Column> fields = new ArrayList<>();
    for (int i : groupSet) {
      Field field = rel.getFields().get(i);
      fields.add((Column) field);
    }
    return fields;
  }

  //Describes all the columns that are referred to in this input
  @Override
  public List<Column> getReferences() {
    return null;
  }
}
