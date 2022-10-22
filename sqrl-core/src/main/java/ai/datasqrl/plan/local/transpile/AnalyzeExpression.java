package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.plan.local.transpile.AnalyzeStatement.Context;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTableField;
import ai.datasqrl.plan.local.transpile.AnalyzeExpression.ExpressionContext;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import lombok.Getter;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

public class AnalyzeExpression extends SqlBasicVisitor<ExpressionContext> {

  private final boolean allowPaths;
  private final Context context;
  @Getter
  private final Map<SqlIdentifier, ResolvedTableField> resolvedFields = new HashMap<>();
  private final boolean allowSystemFields;
  private final AnalyzeStatement analyzeStatement;

  public AnalyzeExpression(boolean allowPaths, Context context, boolean allowSystemFields,
      AnalyzeStatement analyzeStatement) {
    this.allowPaths = allowPaths;
    this.context = context;
    this.allowSystemFields = allowSystemFields;
    this.analyzeStatement = analyzeStatement;
  }

  @Override
  public ExpressionContext visit(SqlCall call) {
    //todo check for subquery in expression
    //todo: Agg + identifier
    switch (call.getKind()) {
      case AS:
        return visitAs(call);
      case SELECT:
      case UNION:
      case INTERSECT:
      case EXCEPT:
        analyzeStatement.accept(call);
        return null;
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
