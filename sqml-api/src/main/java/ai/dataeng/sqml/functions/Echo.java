package ai.dataeng.sqml.functions;

import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.type.StringType;
import ai.dataeng.sqml.type.Type;
import java.sql.Connection;

public class Echo extends SqmlFunction {
  public Echo() {
    super("echo", new StringType(), false);
  }

  public static String fn(Connection connection) {
    return "echo";
  }
}
