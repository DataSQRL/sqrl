package com.datasqrl.planner.dag.plan;

import java.util.List;

import com.datasqrl.engine.ExecutionEngine;
import com.datasqrl.planner.tables.SqrlTableFunction;

import lombok.Builder;
import lombok.Singular;
import lombok.Value;

@Value
@Builder
public class ServerStagePlan {

  ExecutionEngine serverEngine;
  /**
   * All accessible functions
   */
  @Singular
  List<SqrlTableFunction> functions;

  /**
   * All mutations
   */
  @Singular
  List<MutationQuery> mutations;

}
