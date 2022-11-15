package ai.datasqrl.plan.calcite.rules;

import org.apache.calcite.adapter.enumerable.EnumerableNestedLoopJoin;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.util.BuiltInMethod;

public class SqrlRelMdRowCount extends RelMdRowCount
    implements BuiltInMetadata.RowCount.Handler {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.ROW_COUNT.method, new SqrlRelMdRowCount());

    public Double getRowCount(Join rel, RelMetadataQuery mq) {
        double rowCount = super.getRowCount(rel,mq);
        if (rel instanceof EnumerableNestedLoopJoin) {
            rowCount = rowCount + 2 * mq.getRowCount(rel.getLeft());
            //Undo the factor 10 penalty from EnumerableNestedLoopJoin
            rowCount = rowCount/100;
        }
        return rowCount;
    }

    public Double getRowCount(RelNode rel, RelMetadataQuery mq) {
        if (rel instanceof Join) return getRowCount((Join)rel,mq);
        return super.getRowCount(rel,mq);
    }

}
