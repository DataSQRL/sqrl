package ai.datasqrl.plan.calcite.rules;

import static org.checkerframework.checker.nullness.NullnessUtil.castNonNull;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.BuiltInMetadata;
import org.apache.calcite.rel.metadata.BuiltInMetadata.RowCount;
import org.apache.calcite.rel.metadata.JaninoRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataQuery;


public class SqrlRelMetadataQuery extends RelMetadataQuery {
  BuiltInMetadata.RowCount.Handler rowCountHandler;
  public SqrlRelMetadataQuery() {
    super();
    this.rowCountHandler = new SqrlRelMdRowCount();
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
}
