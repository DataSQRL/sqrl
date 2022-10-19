package ai.datasqrl.plan.local.transpile;

import org.apache.calcite.sql.*;
import org.apache.calcite.sql.fun.ConvertableFunction;
import org.apache.calcite.sql.validate.*;

public class PathDetectionVisitor extends SqlScopedShuttle {

  private boolean path = false;

  protected PathDetectionVisitor(SqlValidatorScope initialScope) {
    super(initialScope);
  }

  @Override
  public SqlNode visit(SqlIdentifier id) {
    SqlValidatorNamespace namespace = getScope().fullyQualify(id).namespace;
    IdentifierNamespace ins = (IdentifierNamespace) namespace;
    if (ins.resolve() instanceof RelativeTableNamespace) {
      RelativeTableNamespace ans = (RelativeTableNamespace) ins.resolve();
      if (ans.getRelationships().size() > 1) {
        this.path = true;
      }
    }

    return super.visit(id);
  }

  @Override
  protected SqlNode visitScoped(SqlCall call) {
    if (call.getKind() == SqlKind.AS) {
      return call.getOperandList().get(0).accept(this);
    }
    if (call instanceof ConvertableFunction) {
      this.path = true;
    }
    return super.visitScoped(call);
  }

  public boolean hasPath() {
    return path;
  }
}