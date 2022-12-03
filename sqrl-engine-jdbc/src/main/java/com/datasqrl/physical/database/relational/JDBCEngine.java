package com.datasqrl.physical.database.relational;

import com.datasqrl.config.provider.Dialect;
import com.datasqrl.config.util.StreamUtil;
import com.datasqrl.physical.EngineCapability;
import com.datasqrl.physical.EnginePhysicalPlan;
import com.datasqrl.physical.ExecutionEngine;
import com.datasqrl.physical.ExecutionResult;
import com.datasqrl.physical.database.DatabaseEngine;
import com.datasqrl.physical.database.QueryTemplate;
import com.datasqrl.physical.database.relational.ddl.SqlDDLStatement;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.OptimizedDAG;
import com.datasqrl.plan.queries.APIQuery;
import com.google.common.base.Preconditions;
import lombok.extern.slf4j.Slf4j;
import org.apache.calcite.tools.RelBuilder;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.stream.Collectors;

import static com.datasqrl.physical.EngineCapability.*;

@Slf4j
public class JDBCEngine extends ExecutionEngine.Base implements DatabaseEngine {

    public static final EnumMap<Dialect,EnumSet<EngineCapability>> CAPABILITIES_BY_DIALECT = new EnumMap<Dialect, EnumSet<EngineCapability>>(
        Dialect.class);
    static {
        CAPABILITIES_BY_DIALECT.put(Dialect.POSTGRES, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
        CAPABILITIES_BY_DIALECT.put(Dialect.H2, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
    }

    final JDBCEngineConfiguration config;

    public JDBCEngine(JDBCEngineConfiguration configuration) {
        super(JDBCEngineConfiguration.ENGINE_NAME, Type.DATABASE, CAPABILITIES_BY_DIALECT.get(configuration.getDialect()));
        this.config = configuration;
    }

    @Override
    public JDBCEngineConfiguration.ConnectionProvider getConnectionProvider() {
        return config.getConnectionProvider();
    }

    @Override
    public IndexSelectorConfig getIndexSelectorConfig() {
        return IndexSelectorConfigByDialect.of(config.getDialect());
    }

    @Override
    public ExecutionResult execute(EnginePhysicalPlan plan) {
        Preconditions.checkArgument(plan instanceof JDBCPhysicalPlan);
        JDBCPhysicalPlan jdbcPlan = (JDBCPhysicalPlan) plan;
        String dmls = jdbcPlan.getDdlStatements().stream().map(ddl -> ddl.toSql()).collect(Collectors.joining("\n"));
        try (Connection conn = getConnectionProvider().getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                log.trace("Creating: " + dmls);
                stmt.executeUpdate(dmls);
            } catch (SQLException e) {
                throw new RuntimeException("Could not execute SQL query", e);
            }
        } catch (Exception e) {
            throw new RuntimeException("Could not connect to database", e);
        }
        return new ExecutionResult.Message(String.format("Executed %d DDL statements",jdbcPlan.getDdlStatements().size()));
    }

    @Override
    public EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs, RelBuilder relBuilder) {
        List<SqlDDLStatement> ddlStatements = new ArrayList<>();
        MaterializedTableDDLBuilder dbBuilder = new MaterializedTableDDLBuilder();
        List<OptimizedDAG.DatabaseSink> materializedTables = StreamUtil.filterByClass(inputs, OptimizedDAG.DatabaseSink.class)
                .collect(Collectors.toList());
        ddlStatements.addAll(dbBuilder.createTables(materializedTables,true));
        ddlStatements.addAll(dbBuilder.createIndexes(plan.getIndexDefinitions(), true));

        QueryBuilder queryBuilder = new QueryBuilder(this,relBuilder.getRexBuilder());
        Map<APIQuery, QueryTemplate> databaseQueries = queryBuilder.planQueries(plan.getQueries());
        return new JDBCPhysicalPlan(ddlStatements,databaseQueries);
    }



}
