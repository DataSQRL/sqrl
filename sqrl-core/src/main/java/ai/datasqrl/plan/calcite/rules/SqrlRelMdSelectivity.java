package ai.datasqrl.plan.calcite.rules;

import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class SqrlRelMdSelectivity extends RelMdSelectivity {

    @Override
    public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
        if (rel instanceof EnumerableNestedLoopJoin) {
            return RelMdUtil.guessSelectivity(predicate) / 100;
        } else {
            return super.getSelectivity(rel, mq, predicate);
        }
    }
}