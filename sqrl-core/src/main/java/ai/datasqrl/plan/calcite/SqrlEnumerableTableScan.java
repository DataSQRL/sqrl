package ai.datasqrl.plan.calcite;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;

public class SqrlEnumerableTableScan extends TableScan implements EnumerableRel {

  SqrlAbstractTable abstractTable;
  protected SqrlEnumerableTableScan(RelOptCluster cluster,
      RelTraitSet traitSet,
      List<RelHint> hints,
      RelOptTable table) {
    super(cluster, traitSet.plus(EnumerableConvention.INSTANCE), hints, table);
  }


  @Override
  public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
    PhysType physType =
        PhysTypeImpl.of(
            implementor.getTypeFactory(),
            getRowType(),
            pref.preferArray());

//    if (table instanceof SchemaCalciteTable) {
//      return implementor.result(
//          physType,
//          Blocks.toBlock(
//              Expressions.call(SchemaCalciteTable.class, "project")));
//
//    } else if (table instanceof SourceCalciteTable) {
      return implementor.result(
          physType,
          Blocks.toBlock(
              Expressions.call(table.getExpression(SourceCalciteTable.class),
                  "project", implementor.getRootExpression())));
//
//    }
//
//    throw new RuntimeException();
  }
}
