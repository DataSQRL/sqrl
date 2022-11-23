package ai.datasqrl.plan.calcite.memory.rel;

import ai.datasqrl.plan.calcite.memory.table.DataTable;
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

/**
 * Contains code generation routine that executes the in-memory schema that contains data
 */
public class InMemoryEnumerableTableScan extends TableScan implements EnumerableRel {

  public InMemoryEnumerableTableScan(RelOptCluster cluster,
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


      return implementor.result(
          physType,
          Blocks.toBlock(
              Expressions.call(table.getExpression(DataTable.class),
                  "project", implementor.getRootExpression())));
  }
}
