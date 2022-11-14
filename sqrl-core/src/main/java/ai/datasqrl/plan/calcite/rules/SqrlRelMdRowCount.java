package ai.datasqrl.plan.calcite.rules;

import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataQuery;

public class SqrlRelMdRowCount extends RelMdRowCount
    implements BuiltInMetadata.RowCount.Handler {

    public Double getRowCount(Join rel, RelMetadataQuery mq) {
        double rowCount = super.getRowCount(rel,mq);
        if (rel instanceof EnumerableNestedLoopJoin) {
            rowCount = rowCount + 2 * mq.getRowCount(rel.getLeft());
            //Undo the factor 10 penalty from EnumerableNestedLoopJoin
            rowCount = rowCount/10;
        }
        return rowCount;
    }

}
