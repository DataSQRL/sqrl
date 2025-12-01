//DEPS org.apache.flink:flink-table-common:2.1.0

import org.apache.flink.table.functions.ScalarFunction;

/**
 * User-defined scalar function for use in SQRL scripts.
 */
public class __udfname__ extends ScalarFunction {

  public String eval(Integer x, String y) {
    if (x == null) {
      return y;
    } else if (y == null) {
      return null;
    }

    return y.repeat(x);
  }
}
