package ai.datasqrl.plan.local.generate;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

public class AddConditionsToImplicitJoins extends SqlShuttle {

  private final AnalyzeStatement analyzeStatement;

  public AddConditionsToImplicitJoins(AnalyzeStatement analyzeStatement) {

    this.analyzeStatement = analyzeStatement;
  }

  public SqlNode accept(SqlNode node) {
    return node.accept(this);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    switch (call.getKind()) {
      case JOIN:
        return addCondition((SqlJoin)call);

    }

    return super.visit(call);
  }

  private SqlNode addCondition(SqlJoin join) {
    //1. get

    return join;
  }
}
