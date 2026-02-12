///usr/bin/env jbang "$0" "$@" ; exit $?
import org.apache.flink.table.functions.ScalarFunction;

public class MyScalarFunction extends ScalarFunction {

  public Long eval(Long a, Long b) {
    return a + b;
  }
}
