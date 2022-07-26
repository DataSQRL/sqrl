package ai.datasqrl.plan.calcite;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

public class DeepSqlNodeClone extends SqlShuttle {

  @Override
  public SqlNode visit(SqlIdentifier id) {
    return id.clone(id.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlLiteral literal) {
    return literal.clone(literal.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    return type.clone(type.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlDynamicParam param) {
    return param.clone(param.getParserPosition());
  }

  @Override
  public SqlNode visit(SqlIntervalQualifier intervalQualifier) {
    return intervalQualifier.clone(intervalQualifier.getParserPosition());
  }
}
