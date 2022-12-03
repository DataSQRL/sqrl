package com.datasqrl.physical.database.inmemory;

import com.datasqrl.config.provider.DatabaseConnectionProvider;
import com.datasqrl.physical.EnginePhysicalPlan;
import com.datasqrl.physical.ExecutionEngine;
import com.datasqrl.physical.ExecutionResult;
import com.datasqrl.physical.database.DatabaseEngine;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.OptimizedDAG;
import org.apache.calcite.tools.RelBuilder;

import java.util.EnumSet;
import java.util.List;

import static com.datasqrl.physical.EngineCapability.*;

/**
 * Just a stub for now - not yet functional
 */
public class InMemoryDatabase extends ExecutionEngine.Base implements DatabaseEngine {


    public InMemoryDatabase() {
        super(InMemoryDatabaseConfiguration.ENGINE_NAME, Type.DATABASE, EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
    }

    @Override
    public ExecutionResult execute(EnginePhysicalPlan plan) {
        throw new UnsupportedOperationException();
    }

    @Override
    public EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs, RelBuilder relBuilder) {
        throw new UnsupportedOperationException();
    }

    @Override
    public DatabaseConnectionProvider getConnectionProvider() {
        return new ConnectionProvider();
    }

    private class ConnectionProvider implements DatabaseConnectionProvider {

    }

    @Override
    public IndexSelectorConfig getIndexSelectorConfig() {
        return new IndexSelectorConfig() {
            @Override
            public double getCostImprovementThreshold() {
                return 0;
            }

            @Override
            public EnumSet<IndexDefinition.Type> supportedIndexTypes() {
                return EnumSet.noneOf(IndexDefinition.Type.class);
            }

            @Override
            public int maxIndexColumns(IndexDefinition.Type indexType) {
                return 0;
            }

            @Override
            public double relativeIndexCost(IndexDefinition index) {
                return 0;
            }
        };
    }
}
