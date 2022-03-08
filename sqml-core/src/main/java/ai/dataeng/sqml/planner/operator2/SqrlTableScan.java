package ai.dataeng.sqml.planner.operator2;

import ai.dataeng.sqml.planner.Column;
import ai.dataeng.sqml.planner.Field;
import ai.dataeng.sqml.planner.operator.ShadowingContainer;
import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import lombok.Getter;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.SqrlRelDataTypeField;
import org.apache.calcite.schema.SqrlCalciteTable;
import org.apache.calcite.schema.Table;

public class SqrlTableScan extends TableScan implements SqrlRelNode {
  @Getter
  private final boolean isContextTable;

  public SqrlTableScan(RelOptCluster cluster, RelTraitSet traitSet,
      List<RelHint> hints, RelOptTable table, boolean isContextTable) {
    super(cluster, traitSet, hints, table);
    this.isContextTable = isContextTable;
  }
  public static SqrlTableScan create(RelOptCluster cluster,
      final RelOptTable relOptTable, List<RelHint> hints) {
    return create(cluster, relOptTable, hints, false);
  }

  public static SqrlTableScan create(RelOptCluster cluster,
      final RelOptTable relOptTable, List<RelHint> hints, boolean isContextTable) {
    final Table table = relOptTable.unwrap(Table.class);
    final RelTraitSet traitSet =
        cluster.traitSetOf(Convention.NONE)
            .replaceIfs(RelCollationTraitDef.INSTANCE, () -> {
              if (table != null) {
                return table.getStatistic().getCollations();
              }
              return ImmutableList.of();
            });
    return new SqrlTableScan(cluster, traitSet, hints, relOptTable, isContextTable);
  }

  @Override
  public List<Field> getFields() {
    List s = new ArrayList();
    s.addAll(table.getRowType().getFieldList().stream()
        .map(f->(SqrlRelDataTypeField) f)
        .map(f->f.getPath().getLastField()) //todo: wrong but ?
        .collect(Collectors.toList()));
    return s;
  }

  @Override
  public List<Column> getPrimaryKeys() {
    return ((SqrlRelNode)table.unwrap(SqrlCalciteTable.class).getSqrlTable().getNode()).getPrimaryKeys();
  }

  @Override
  public List<Column> getReferences() {
    return null;
  }

  @Override
  public RelDataType deriveRowType() {
    return super.deriveRowType();
  }

  @Override public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw)
//        .item("fields", this.getFields().stream().map(e->e.getName()).collect(Collectors.toList()))
        ;
  }
}
