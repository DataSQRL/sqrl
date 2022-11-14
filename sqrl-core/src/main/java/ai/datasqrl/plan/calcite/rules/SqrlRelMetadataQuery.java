package ai.datasqrl.plan.calcite.rules;

import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class SqrlRelMetadataQuery extends RelMetadataQuery {

  @Override
  public Double getRowCount(RelNode rel) {
    if (rel instanceof Join) {
      Join join = (Join) rel;
      double rowCount = super.getRowCount(rel);
      if (rel instanceof EnumerableNestedLoopJoin) {
        rowCount = rowCount + 2 * super.getRowCount(join.getLeft());
        //Undo the factor 10 penalty from EnumerableNestedLoopJoin
        rowCount = rowCount/10;
      }
      return rowCount;
    }

    return super.getRowCount(rel);
  }
}
