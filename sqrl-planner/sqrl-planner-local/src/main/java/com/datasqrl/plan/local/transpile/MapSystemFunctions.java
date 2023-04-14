package com.datasqrl.plan.local.transpile;

import com.datasqrl.canonicalizer.Name;
import com.datasqrl.plan.local.transpile.AnalyzeStatement.Analysis;
import java.util.Optional;
import lombok.AllArgsConstructor;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlShuttle;

@AllArgsConstructor
public class MapSystemFunctions extends SqlShuttle {
  Analysis analysis;

  @Override
  public SqlNode visit(SqlCall call) {
    //check if call can be aliased
    if (call.getOperator() instanceof SqlFunction) {
      Optional<SqlFunction> fnc = analysis.getNs().translateFunction(Name.system(call.getOperator().getName()));
      if (fnc.isPresent()) {
        SqlNode[] operands = call.getOperandList().stream()
            .map(f->f.accept(this))
            .toArray(SqlNode[]::new);

        return new SqlBasicCall(fnc.get(), operands, call.getParserPosition());
      }
    }

    return super.visit(call);
  }
}
