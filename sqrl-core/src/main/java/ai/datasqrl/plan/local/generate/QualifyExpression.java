package ai.datasqrl.plan.local.generate;

import ai.datasqrl.plan.local.generate.AnalyzeStatement.Context;
import ai.datasqrl.plan.local.generate.AnalyzeStatement.ResolvedTableField;
import ai.datasqrl.plan.local.generate.QualifyExpression.ExpressionContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

public class QualifyExpression extends SqlBasicVisitor<ExpressionContext> {

  private final boolean allowPaths;
  private final Context context;
  @Getter
  private final Map<SqlIdentifier, ResolvedTableField> resolvedFields = new HashMap<>();
  private final boolean allowSystemFields;
  public QualifyExpression(boolean allowPaths, Context context, boolean allowSystemFields) {
    this.allowPaths = allowPaths;
    this.context = context;
    this.allowSystemFields = allowSystemFields;
  }

  @Override
  public ExpressionContext visit(SqlCall call) {
    //todo check for subquery in expression
    //todo: Agg + identifier
    switch (call.getKind()) {
      case AS:
        return visitAs(call);

    }

    return super.visit(call);
  }

  private ExpressionContext visitAs(SqlCall call) {
    call.getOperandList().get(0).accept(this);

    return null;
  }

  @Override
  public ExpressionContext visit(SqlIdentifier id) {
    Optional<ResolvedTableField> field = context.resolveField(id.names, allowSystemFields);
    if (field.isEmpty()) {
      //it may not be a field, skip it
      return super.visit(id);
    }
    this.resolvedFields.put(id, field.get());

    return super.visit(id);
  }


  class ExpressionContext {

  }
}
