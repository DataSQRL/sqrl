package ai.dataeng.sqml.functions;

import ai.dataeng.sqml.function.SqmlFunction;
import ai.dataeng.sqml.type.StringType;
import ai.dataeng.sqml.type.Type;
import java.sql.Connection;
import java.sql.SQLException;

public class EchoAgg extends SqmlFunction implements org.h2.api.AggregateFunction {

  public EchoAgg() {
    super("echoagg", new StringType(), true);
  }

  @Override
  public void init(Connection connection) throws SQLException {

  }

  @Override
  public int getType(int[] ints) throws SQLException {
    return 0;
  }

  @Override
  public void add(Object o) throws SQLException {

  }

  @Override
  public Object getResult() throws SQLException {
    return "echo-agg";
  }
}
