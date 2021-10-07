package ai.dataeng.sqml.functions;

import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.schema2.basic.StringType;
import java.sql.Connection;

public class Echo extends SqmlFunction {
  public Echo() {
    super("echo", new StringType(), false);
  }

  public static String fn(Connection connection) {
    return "echo";
  }
}
