package ai.dataeng.sqml.parser.sqrl.function;

import org.apache.calcite.sql.SqlOperator;

public class SqlNativeFunction implements SqrlFunction{
  private final SqlOperator op;

  public SqlNativeFunction(SqlOperator op) {
    this.op = op;
  }

  @Override
  public boolean isAggregate() {
    return op.isAggregator();
  }
}
