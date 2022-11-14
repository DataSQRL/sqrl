package ai.datasqrl.plan.calcite.rules;

import static org.checkerframework.checker.nullness.NullnessUtil.castNonNull;

import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexNode;


public class SqrlRelMetadataQuery extends RelMetadataQuery {
  BuiltInMetadata.RowCount.Handler rowCountHandler;
  BuiltInMetadata.Selectivity.Handler selectivityHandler;
  public SqrlRelMetadataQuery() {
    super();
    this.rowCountHandler = new SqrlRelMdRowCount();
    this.selectivityHandler = new SqrlRelMdSelectivity();
  }

  @Override
  public Double getRowCount(RelNode rel) {
    for (;;) {
      try {
        Double result = rowCountHandler.getRowCount(rel, this);
        return RelMdUtil.validateResult(castNonNull(result));
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        rowCountHandler = revise(e.relClass, BuiltInMetadata.RowCount.DEF);
      }
    }
  }

  @Override
  public @Nullable Double getSelectivity(RelNode rel, @Nullable RexNode predicate) {
    for (;;) {
      try {
        Double result = selectivityHandler.getSelectivity(rel, this, predicate);
        return RelMdUtil.validatePercentage(result);
      } catch (JaninoRelMetadataProvider.NoHandler e) {
        selectivityHandler =
            revise(e.relClass, BuiltInMetadata.Selectivity.DEF);
      }
    }
  }

}
