package ai.datasqrl.plan.calcite.rules;

import ai.datasqrl.plan.calcite.table.VirtualRelationalTable;
import ai.datasqrl.plan.global.IndexCall;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import java.util.List;

public class SqrlRelMdSelectivity extends RelMdSelectivity
    implements BuiltInMetadata.Selectivity.Handler {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.SELECTIVITY.method, new SqrlRelMdSelectivity());

    @Override
    public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
        return super.getSelectivity(rel, mq, predicate);
    }



    public static Double getSelectivity(VirtualRelationalTable table, List<IndexCall.IndexColumn> constraints) {
        //TODO: use actual selectivity statistics from table
        double selectivity = 1.0d;
        for (IndexCall.IndexColumn col : constraints) {
            switch (col.getType()) {
                case EQUALITY:
                    selectivity *= 0.1;
                    break;
                case COMPARISON:
                    selectivity *= 0.4;
                    break;
            }
        }
        return selectivity;
    }

}