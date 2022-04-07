package ai.dataeng.sqml.planner;

import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelShuttleImpl;

@AllArgsConstructor
public class DagAssembler extends RelShuttleImpl {

//  @Override
//  public RelNode visit(TableScan scan) {
//    return ((SqrlCalciteTable) scan.getRowType()).getSqrlTable().getRelNode();
//  }
}
