package ai.datasqrl.plan.calcite.rules;

import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;

public class SqrlRelMdSelectivity extends RelMdSelectivity
    implements BuiltInMetadata.Selectivity.Handler {

    @Override
    public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
        return super.getSelectivity(rel, mq, predicate);
    }
}