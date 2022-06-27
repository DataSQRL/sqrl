package ai.datasqrl.function.calcite;

import ai.datasqrl.function.SqrlAwareFunction;
import ai.datasqrl.parse.tree.name.Name;
import org.apache.calcite.sql.SqlOperator;

/**
 * Maps a calcite function to a sqrl function
 */
public class CalciteFunctionProxy implements SqrlAwareFunction {

  private final SqlOperator op;

  public CalciteFunctionProxy(SqlOperator op) {
    this.op = op;
  }

  @Override
  public Name getSqrlName() {
    return Name.system(op.getName());
  }

  @Override
  public boolean isAggregate() {
    return op.isAggregator();
  }

  @Override
  public boolean requiresOver() {
    return op.requiresOver();
  }

  @Override
  public boolean isDeterministic() {
    return op.isDeterministic();
  }

  @Override
  public boolean isTimestampPreserving() {
    return false;
  }

  @Override
  public String toString() {
    return "CalciteFunctionProxy{" +
        "name=" + getSqrlName() +
        '}';
  }
}
