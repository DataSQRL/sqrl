package ai.datasqrl.plan.local.generate;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.util.SqlShuttle;

public class SqlShuttle2<C> {
  SqlShuttle sqlShuttle = new SqlShuttle() {
    @Override
    public SqlNode visit(SqlCall call) {
      SqlNode node = visitCall(call, contexts.pop());;
      return super.visit((SqlCall)node);
    }
  };

  Stack<C> contexts = new Stack<>();

  public SqlNode visitCall(SqlCall call, C context) {
    return null;
  }

  public SqlNode visit(SqlLiteral literal, C context) {
    return literal;
  }

  public SqlNode visit(SqlIdentifier id, C context) {
    return id;
  }

  public SqlNode visit(SqlDataTypeSpec type, C context) {
    return type;
  }

  public SqlNode visit(SqlDynamicParam param, C context) {
    return param;
  }

  public SqlNode visit(SqlIntervalQualifier intervalQualifier, C context) {
    return intervalQualifier;
  }

  public SqlNode visit(SqlNodeList nodeList, C context) {

    return nodeList;
  }

  protected SqlNode rewrite(SqlNode node, C context) {
    contexts.push(context);

    return node.accept(sqlShuttle);
  }
}
