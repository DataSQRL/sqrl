package com.datasqrl.engine.database.relational;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.JdbcDialect;
import com.datasqrl.config.PackageJson;
import com.datasqrl.config.PackageJson.EmptyEngineConfig;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.engine.database.QueryTemplate;
import com.datasqrl.plan.queries.IdentifiedQuery;
import com.google.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.NonNull;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class DuckDBEngine extends AbstractJDBCQueryEngine {

  @Inject
  public DuckDBEngine(
      @NonNull PackageJson json,
      ConnectorFactoryFactory connectorFactory) {
    super(DuckDBEngineFactory.ENGINE_NAME, json.getEngines().getEngineConfig(DuckDBEngineFactory.ENGINE_NAME)
            .orElseGet(()-> new EmptyEngineConfig(DuckDBEngineFactory.ENGINE_NAME)),
        connectorFactory);
  }

  @Override
  protected JdbcDialect getDialect() {
    return JdbcDialect.Postgres;
  }

  @Override
  public Map<IdentifiedQuery, QueryTemplate> updateQueries(
      ConnectorFactoryFactory connectorFactory, EngineConfig connectorConfig, Map<IdentifiedQuery, QueryTemplate> queries) {
    for (Map.Entry<IdentifiedQuery, QueryTemplate> entry : queries.entrySet()) {
      RelNode relNode = entry.getValue().getRelNode();

      RelNode replaced = relNode.accept(new RelShuttleImpl() {
        @Override
        public RelNode visit(TableScan scan) {
          //look for iceberg, convert
          //iceberg_scan('/Users/henneberger/sqrl/sqrl-testing/sqrl-flink-1.18/src/test/default/my_table', allow_moved_paths = true)
          Map<String, Object> map = connectorFactory.getConfig("iceberg").toMap();
          String warehouse =(String) map.get("warehouse");
          String catalogName =(String) map.get("catalog-name");
          RexBuilder rexBuilder = new RexBuilder(new TypeFactory());
          if (warehouse.startsWith("file://")) {
            warehouse = warehouse.substring(7);
          }

          RexNode allowMovedPaths = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
              rexBuilder.makeFlag(Params.ALLOW_MOVED_PATHS),
              rexBuilder.makeLiteral(true));
          RexNode rexNode = rexBuilder.makeCall(lightweightOp("iceberg_scan"),
              rexBuilder.makeLiteral(warehouse+"/default_database/" + scan.getTable().getQualifiedName().get(0)),
              allowMovedPaths);


          return new LogicalTableFunctionScan(scan.getCluster(), scan.getTraitSet(), List.of(), rexNode,
              Object.class, scan.getRowType(), Set.of());
        }
      });

      queries.put(entry.getKey(), new QueryTemplate("duckdb", replaced));
    }


    return queries;
  }

  enum Params {
    ALLOW_MOVED_PATHS
  }
}
