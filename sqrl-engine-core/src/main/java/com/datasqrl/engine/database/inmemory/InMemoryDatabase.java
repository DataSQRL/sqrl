/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.inmemory;

import static com.datasqrl.engine.EngineCapability.GLOBAL_SORT;
import static com.datasqrl.engine.EngineCapability.MULTI_RANK;
import static com.datasqrl.engine.EngineCapability.NOW;

import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.plan.global.OptimizedDAG;
import java.util.EnumSet;
import java.util.List;
import org.apache.calcite.tools.RelBuilder;

/**
 * Just a stub for now - not yet functional
 */
public class InMemoryDatabase extends ExecutionEngine.Base implements DatabaseEngine {


  public InMemoryDatabase() {
    super(InMemoryDatabaseConfiguration.ENGINE_NAME, Type.DATABASE,
        EnumSet.of(NOW, GLOBAL_SORT, MULTI_RANK));
  }

  @Override
  public ExecutionResult execute(EnginePhysicalPlan plan) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnginePhysicalPlan plan(OptimizedDAG.StagePlan plan, List<OptimizedDAG.StageSink> inputs,
      RelBuilder relBuilder, TableSink errorSink) {
    throw new UnsupportedOperationException();
  }

  @Override
  public IndexSelectorConfig getIndexSelectorConfig() {
    return new IndexSelectorConfig() {
      @Override
      public double getCostImprovementThreshold() {
        return 0.95;
      }

      @Override
      public int maxIndexColumnSets() {
        return 100;
      }

      @Override
      public EnumSet<IndexDefinition.Type> supportedIndexTypes() {
        return EnumSet.of(IndexDefinition.Type.HASH, IndexDefinition.Type.BTREE);
      }

      @Override
      public int maxIndexColumns(IndexDefinition.Type indexType) {
        switch (indexType) {
          case HASH:
            return 1;
          default:
            return 6;
        }
      }

      @Override
      public double relativeIndexCost(IndexDefinition index) {
        return 1;
      }
    };
  }
}
