/*
 * Copyright (c) 2021, DataSQRL. All rights reserved. Use is subject to license terms.
 */
package com.datasqrl.engine.database.inmemory;

import static com.datasqrl.engine.EngineFeature.STANDARD_DATABASE;

import com.datasqrl.calcite.SqrlFramework;
import com.datasqrl.engine.EnginePhysicalPlan;
import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.engine.ExecutionResult;
import com.datasqrl.engine.database.DatabaseEngine;
import com.datasqrl.engine.pipeline.ExecutionPipeline;
import com.datasqrl.error.ErrorCollector;
import com.datasqrl.function.SqrlTimeTumbleFunction;
import com.datasqrl.io.tables.TableSink;
import com.datasqrl.plan.global.IndexDefinition;
import com.datasqrl.plan.global.IndexSelectorConfig;
import com.datasqrl.function.IndexType;
import com.datasqrl.plan.global.PhysicalDAGPlan.StagePlan;
import com.datasqrl.plan.global.PhysicalDAGPlan.StageSink;
import com.datasqrl.plan.global.PhysicalDAGPlan;
import com.datasqrl.util.FunctionUtil;
import org.apache.flink.table.functions.FunctionDefinition;

import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/**
 * Just a stub for now - not yet functional
 */
public class InMemoryDatabase extends ExecutionEngine.Base implements DatabaseEngine {


  public InMemoryDatabase() {
    super(InMemoryDatabaseConfiguration.ENGINE_NAME, Type.DATABASE, STANDARD_DATABASE);
  }

  @Override
  public CompletableFuture<ExecutionResult> execute(EnginePhysicalPlan plan, ErrorCollector errors) {
    throw new UnsupportedOperationException();
  }

  @Override
  public EnginePhysicalPlan plan(StagePlan plan, List<StageSink> inputs,
                                 ExecutionPipeline pipeline, SqrlFramework relBuilder, TableSink errorSink,
      ErrorCollector errorCollector) {
    throw new UnsupportedOperationException();
  }

  public boolean supports(FunctionDefinition function) {
      return FunctionUtil.getTimestampPreservingFunction(function)
          .isEmpty();
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
      public EnumSet<IndexType> supportedIndexTypes() {
        return EnumSet.of(IndexType.HASH, IndexType.BTREE);
      }

      @Override
      public int maxIndexColumns(IndexType indexType) {
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
