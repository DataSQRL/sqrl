package ai.datasqrl.plan.calcite.rules;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.*;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

public class SqrlRelMdSelectivity extends RelMdSelectivity
    implements BuiltInMetadata.Selectivity.Handler {

    public static final RelMetadataProvider SOURCE =
            ReflectiveRelMetadataProvider.reflectiveSource(
                    BuiltInMethod.SELECTIVITY.method, new SqrlRelMdSelectivity());

    @Override
    public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
        return super.getSelectivity(rel, mq, predicate);
    }
}