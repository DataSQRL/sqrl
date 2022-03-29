package ai.dataeng.sqml.parser.sqrl.calcite;

import ai.dataeng.sqml.parser.CalciteTools;
import ai.dataeng.sqml.parser.sqrl.LogicalDag;
import java.util.Optional;
import org.apache.calcite.jdbc.SqrlTypeFactory;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.sql.SqrlRelBuilder;

public class CalcitePlanner {

  private final RelOptCluster cluster;
  private final CalciteCatalogReader catalogReader;
  private final LogicalDag logicalDag;

  public CalcitePlanner(LogicalDag logicalDag) {
    SqrlTypeFactory typeFactory = new SqrlTypeFactory();
    this.cluster = CalciteTools.createHepCluster(typeFactory);
    this.catalogReader = CalciteTools.getCalciteCatalogReader(logicalDag);
    this.logicalDag = logicalDag;
  }

  public SqrlRelBuilder createRelBuilder() {
    return new SqrlRelBuilder(null, cluster, catalogReader);
  }
}
