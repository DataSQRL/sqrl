package ai.datasqrl.plan.local.transpile;

import ai.datasqrl.config.error.ErrorCode;
import ai.datasqrl.config.error.SqrlAstException;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.Context;
import ai.datasqrl.plan.local.transpile.AnalyzeStatement.ResolvedTableField;
import ai.datasqrl.plan.local.transpile.AnalyzeExpression.ExpressionContext;
import ai.datasqrl.schema.Field;
import ai.datasqrl.schema.Relationship;
import ai.datasqrl.schema.Relationship.JoinType;
import ai.datasqrl.schema.Relationship.Multiplicity;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
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
    if (id.isStar()) { //skip any count(*)'s
      return super.visit(id);
    }

    Optional<ResolvedTableField> field = context.resolveField(id, id.names);
    if (field.isEmpty()) {
      throw new SqrlAstException(ErrorCode.MISSING_FIELD, id.getParserPosition(),
          "Could not find field: " + id.names);
    }
    Optional<Field> hasToMany = field.get().path.stream()
        .filter(f->f instanceof Relationship)
        .filter(f->((Relationship) f).getMultiplicity() == Multiplicity.MANY)
        .findAny();
    if (hasToMany.isPresent()) {
      throw new SqrlAstException(ErrorCode.TO_MANY_PATH_NOT_ALLOWED, id.getParserPosition(),
          String.format("Field cannot be walked here, has to-many multiplicity: %s", hasToMany.get().getName().getCanonical()));
    }

    this.resolvedFields.put(id, field.get());

    return super.visit(id);
  }


  class ExpressionContext {

  }
}
