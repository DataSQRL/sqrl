package ai.dataeng.sqml.planner.optimize2;

import ai.dataeng.sqml.io.sources.dataset.SourceTable;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.core.TableScan;

public class SqrlLogicalDocument extends TableScan {

  SourceTable sourceTable;

  public SqrlLogicalDocument(RelInput input) {
    super(input);
  }
}
