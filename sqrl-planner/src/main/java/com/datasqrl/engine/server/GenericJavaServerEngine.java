package com.datasqrl.engine.server;

import static com.datasqrl.engine.EngineFeature.STANDARD_DATABASE;

import com.datasqrl.calcite.Dialect;
import com.datasqrl.calcite.QueryPlanner;
import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.calcite.convert.SqlNodeToString.SqlStrings;
import com.datasqrl.config.ConnectorFactory;
import com.datasqrl.config.ConnectorFactoryContext;
import com.datasqrl.config.ConnectorFactoryFactory;
import com.datasqrl.config.EngineFactory.Type;
import com.datasqrl.config.TableConfig;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.engine.stream.flink.plan.SqrlToFlinkSqlGenerator;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.plan.global.DatabaseQueryImpl;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.ReadQuery;
import com.datasqrl.plan.global.PhysicalDAGPlan.ServerStagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.global.PhysicalDAGPlan.WriteQuery;
import com.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.sql.parser.ddl.SqlCreateTable;

/**
 * A generic java server engine.
 */
@Slf4j
public abstract class GenericJavaServerEngine extends ExecutionEngine.Base implements ServerEngine {


//  private final ConnectorFactoryFactory connectorFactory;
//  private final SqrlToFlinkSqlGenerator flinkSqlGenerator;

  public GenericJavaServerEngine(String engineName, ConnectorFactoryFactory connectorFactory,
      SqrlToFlinkSqlGenerator flinkSqlGenerator) {
    super(engineName, Type.SERVER, STANDARD_DATABASE);
//    this.connectorFactory = connectorFactory;
//    this.flinkSqlGenerator = flinkSqlGenerator;
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StagePlan> stagePlans, List<StageSink> inputs, ExecutionPipeline pipeline,
      SqrlFramework framework, ErrorCollector errorCollector) {
    Preconditions.checkArgument(plan instanceof ServerStagePlan);
    Set<StagePlan> upstreamDbStages = stagePlans.stream().filter(s -> s.getStage().getEngine().getType()== Type.DATABASE).collect(
        Collectors.toSet());

    //todo: only generate server queries to plan, move db queries to db physical plan
    // and read from there when building the graphql model.
    QueryPlanner queryPlanner = framework.getQueryPlanner();
//    List<SqlQuery> queries = new ArrayList<>();
//    for (StagePlan plan1 : upstreamDbStages) {
//        for (PhysicalDAGPlan.Query query : plan1.getQueries()) {
//          if (query instanceof ReadQuery) {
//            ReadQuery query1 = (ReadQuery) query;
//            if (query1.getQuery() instanceof APIQuery) {
//              APIQuery apiQuery = (APIQuery) query1.getQuery();
//
//              if (plan1.getStage().getEngine().getName().equals("iceberg")) {
//                //obtain tables, rediscover flink sql sources
//                ConnectorFactory connectorFactory1 = connectorFactory.create(null,
//                    plan1.getStage().getEngine().getName()).get();
//                TableConfig sourceAndSink = connectorFactory1
//                    .createSourceAndSink(new ConnectorFactoryContext(apiQuery.getNameId(),
//                        Map.of("name", apiQuery.getNameId())));
//                SqlCreateTable createTable = flinkSqlGenerator.toCreateTable(apiQuery.getNameId(),
//                    apiQuery.getRelNode().getRowType(), sourceAndSink, true);
//
//                String sql = queryPlanner.relToString(Dialect.FLINK, apiQuery.getRelNode())
//                    .getSql();
//
//                String table = queryPlanner.sqlToString(Dialect.FLINK, () -> createTable)
//                    .getSql();
//
//                List<String> sqls = List.of(table, sql);
//
//                FlinkSqlQuery flinkSqlQuery = new FlinkSqlQuery(apiQuery, sqls);
//                queries.add(flinkSqlQuery);
//              } else if (plan1.getStage().getEngine().getName().equals("postgres")) {
//                String sql = queryPlanner.relToString(Dialect.POSTGRES, apiQuery.getRelNode())
//                    .getSql();
//                PostgresSqlQuery flinkSqlQuery = new PostgresSqlQuery(apiQuery, sql);
//                queries.add(flinkSqlQuery);
//              } else {
//                throw new RuntimeException();
//              }
//            } else if (query1.getQuery() instanceof DatabaseQueryImpl) {
//              DatabaseQueryImpl dbQuery = (DatabaseQueryImpl) query1.getQuery();
//              String sql = queryPlanner.relToString(Dialect.CALCITE, dbQuery.getPlannedRelNode())
//                  .getSql();
//              queries.add(new CalciteDbSqlQuery(dbQuery, sql));
//            }
//
//          } else if (query instanceof WriteQuery) {
//            throw new RuntimeException();
//          } else {
//            throw new RuntimeException();
//          }
//        }
//    }

    List<CalciteSqlQuery> queries = new ArrayList<>();
    //server queries
    for (PhysicalDAGPlan.Query query : plan.getQueries()) {
      if (((ReadQuery)query).getQuery() instanceof APIQuery) {
        APIQuery apiQuery = (APIQuery)((ReadQuery)query).getQuery();
        String sql = queryPlanner.relToString(Dialect.CALCITE, apiQuery.getRelNode())
            .getSql();
        queries.add(new CalciteSqlQuery(
            apiQuery.getNamePath().getDisplay(), sql));

      } else {
        throw new RuntimeException();
      }

    }

    return new ServerPhysicalPlan(/*Will set later after queries are generated*/null, queries);
  }


//
//  @Value
//  private class PostgresSqlQuery implements SqlQuery {
//    APIQuery apiQuery;
//    String sqls;
//  }
  @Value
  protected class CalciteSqlQuery {
    String path;
    String sql;
  }

//  @Value
//  private class CalciteDbSqlQuery implements SqlQuery {
//    DatabaseQueryImpl apiQuery;
//    String sqls;
//  }
//
//  @Value
//  private class FlinkSqlQuery implements SqlQuery {
//    APIQuery apiQuery;
//    List<String> sqls;
//  }
//
//  interface SqlQuery {
//
//  }
}
