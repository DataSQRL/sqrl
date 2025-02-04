package com.datasqrl.engine.database.relational;

import static com.datasqrl.function.CalciteFunctionUtil.lightweightOp;

import com.datasqrl.calcite.convert.PostgresRelToSqlNode;
import com.datasqrl.calcite.convert.PostgresSqlNodeToString;
import com.datasqrl.calcite.dialect.ExtendedPostgresSqlDialect;
import com.datasqrl.calcite.type.TypeFactory;
import com.datasqrl.config.PackageJson.EngineConfig;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.v2.dag.plan.MaterializationStagePlan.Query;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableFunctionScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

public class DuckDbStatementFactory extends AbstractJdbcStatementFactory {

  private final EngineConfig engineConfig;
  private Map<String,String> connectorOptions;

  public DuckDbStatementFactory(EngineConfig engineConfig) {
    super(new PostgresRelToSqlNode(), new PostgresSqlNodeToString()); //Iceberg does not support queries
    this.engineConfig = engineConfig;
  }

  @Override
  public JdbcStatement createTable(JdbcEngineCreateTable createTable) {
    /* We make the simplifying assumptions that the connector options we care about in
       createQuery relnode rewriting are the same for all tables.
       To make this more robust, we would look them up by name
     */

    this.connectorOptions = createTable.getTable().getConnectorOptions();
    return super.createTable(createTable);
  }

  @Override
  protected SqlDataTypeSpec getSqlType(RelDataType type) {
    return ExtendedPostgresSqlDialect.DEFAULT.getCastSpec(type);
  }

  @Override
  public QueryResult createQuery(Query query, boolean withView) {
    RelNode relNode = query.getRelNode();
    RelNode replaced = relNode.accept(new RelShuttleImpl() {
      @Override
      public RelNode visit(TableScan scan) {
        Map<String, String> map = connectorOptions;
        String warehouse =map.get("warehouse");
        String databaseName =map.get("database-name") == null ? "default_database" :
            map.get("database-name");
        RexBuilder rexBuilder = new RexBuilder(new TypeFactory());
        if (warehouse.startsWith("file://")) {
          warehouse = warehouse.substring(7);
        }

        RexNode allowMovedPaths = rexBuilder.makeCall(SqlStdOperatorTable.EQUALS,
            rexBuilder.makeFlag(Params.ALLOW_MOVED_PATHS),
            rexBuilder.makeLiteral(true));
        RexNode rexNode = rexBuilder.makeCall(lightweightOp("iceberg_scan"),
            rexBuilder.makeLiteral(warehouse+"/"+databaseName+"/" + scan.getTable().getQualifiedName().get(0)),
            allowMovedPaths);
        return new LogicalTableFunctionScan(scan.getCluster(), scan.getTraitSet(), List.of(), rexNode,
            Object.class, scan.getRowType(), Set.of());
      }
    });

    return createQuery(query.getFunction().getFunctionCatalogName(), replaced, false);
  }

  @Override
  public JdbcStatement addIndex(IndexDefinition indexDefinition) {
    throw new UnsupportedOperationException("DuckDB does not support indexes");
  }

  enum Params {
    ALLOW_MOVED_PATHS
  }
}
