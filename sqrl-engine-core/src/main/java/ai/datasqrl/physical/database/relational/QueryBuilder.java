package ai.datasqrl.physical.database.relational;

import ai.datasqrl.config.provider.DatabaseConnectionProvider;
import ai.datasqrl.function.SqrlFunction;
import ai.datasqrl.function.builtin.time.StdTimeLibraryImpl;
import ai.datasqrl.physical.database.QueryTemplate;
import ai.datasqrl.plan.calcite.util.CalciteUtil;
import ai.datasqrl.plan.global.OptimizedDAG;
import ai.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import lombok.AllArgsConstructor;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.dialect.PostgresqlSqlDialect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@AllArgsConstructor
public class QueryBuilder {

  private JDBCEngine engine;
  private RexBuilder rexBuilder;

  public Map<APIQuery, QueryTemplate> planQueries(List<? extends OptimizedDAG.Query> databaseQueries) {
    DatabaseConnectionProvider connectionProvider = engine.getConnectionProvider();
    Map<APIQuery, QueryTemplate> resultQueries = new HashMap<>();
    for (OptimizedDAG.Query query : databaseQueries) {
      Preconditions.checkArgument(query instanceof OptimizedDAG.ReadQuery);
      OptimizedDAG.ReadQuery rquery = (OptimizedDAG.ReadQuery)query;
      resultQueries.put(rquery.getQuery(), planQuery(rquery, connectionProvider));
    }
    return resultQueries;
  }

  private QueryTemplate planQuery(OptimizedDAG.ReadQuery query, DatabaseConnectionProvider connectionProvider) {
    RelNode relNode = query.getRelNode();
    relNode = CalciteUtil.applyRexShuttleRecursively(relNode,new FunctionNameRewriter());
    return new QueryTemplate(relNode, connectionProvider);
  }

  private SqlDialect getCalciteDialect() {
    switch (engine.config.dialect) {
      case POSTGRES: return PostgresqlSqlDialect.DEFAULT;
      default: throw new UnsupportedOperationException("Not a supported dialect: " + engine.config.dialect);
    }
  }

  private class FunctionNameRewriter extends RexShuttle {

    @Override
    public RexNode visitCall(RexCall call) {
      boolean[] update = new boolean[]{false};
      List<RexNode> clonedOperands = this.visitList(call.operands, update);
      SqlOperator operator = call.getOperator();
      RelDataType datatype = call.getType();
      Optional<SqrlFunction> sqrlFunction = SqrlFunction.unwrapSqrlFunction(operator);
      if (sqrlFunction.isPresent()) {
        update[0] = true;
        if (sqrlFunction.get().equals(StdTimeLibraryImpl.NOW)) {
          Preconditions.checkArgument(clonedOperands.isEmpty());
          int precision = datatype.getPrecision();
          operator = SqlStdOperatorTable.CURRENT_TIMESTAMP;
          //clonedOperands = List.of(rexBuilder.makeLiteral)
        } else {
          throw new UnsupportedOperationException("Function not supported in database: " + operator);
        }
      }
      return update[0] ? rexBuilder.makeCall(datatype,operator,clonedOperands) : call;
    }

  }

}
