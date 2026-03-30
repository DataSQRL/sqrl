import org.apache.flink.table.functions.ScalarFunction;

/**
 * User-defined scalar function for use in SQRL scripts.
 */
public class __fnname__ extends ScalarFunction {

  public String eval(Integer x, String y) {
    if (x == null) {
      return y;
    } else if (y == null) {
      return null;
    }

    return y.repeat(x);
  }
}
