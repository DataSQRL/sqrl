package ai.datasqrl.plan.nodes;

import lombok.Getter;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.impl.AbstractTable;

public class RelNodeTable extends AbstractTable implements ScannableTable {

  RelDataType relDataType;
  @Getter
  private final RelNode relNode;

  private final Statistic statistic;

  public RelNodeTable(RelNode relNode, Statistic statistic) {
    this.relNode = relNode;
    this.relDataType = relNode.getRowType();
    this.statistic = statistic;
  }

  @Override
  public Statistic getStatistic() {
    return statistic;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    return relDataType;
  }

  @Override
  public Enumerable<Object[]> scan(DataContext dataContext) {
    return Linq4j.asEnumerable(new Object[][]{});
  }
}
