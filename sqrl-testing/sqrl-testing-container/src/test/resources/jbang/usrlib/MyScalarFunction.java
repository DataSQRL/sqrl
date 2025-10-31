//DEPS org.apache.flink:flink-table-common:2.1.0

import org.apache.flink.table.functions.ScalarFunction;

public class MyScalarFunction extends ScalarFunction {

  public Long eval(Long a, Long b) {
    return a + b;
  }
}
